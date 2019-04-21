use std::ffi::{OsStr, OsString};
use std::fs::{remove_file, set_permissions, write, File, Permissions};
use std::io::{BufWriter, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use chrono::{offset::Local, DateTime};
use failure::{bail, Error, ResultExt, SyncFailure};
use log::{error, info, warn};
use serde::de::DeserializeOwned;
use serde_derive::Deserialize;
use serde_xml_rs::from_str as from_xml_str;
use serde_yaml::from_reader as from_yaml_reader;
use simplelog::{Config as LogConfig, LevelFilter, SimpleLogger};
use tempfile::{NamedTempFile, TempDir};

fn main() -> Result<(), Error> {
    SimpleLogger::init(
        LevelFilter::Info,
        LogConfig {
            time: None,
            target: None,
            ..Default::default()
        },
    )
    .unwrap();

    download_util().context("Failed to download idevsutil_dedup")?;

    let config = read_config().context("Failed to read config")?;
    let srv_ip = get_server_ip(&config).context("Failed to determine server IP")?;
    let dev_id = get_device_id(&config, &srv_ip).context("Failed to determine device ID")?;

    info!(
        "Starting backup from {} to {} ({}) at {}...",
        get_hostname()?,
        config.device_name,
        dev_id,
        srv_ip
    );

    let starttime = Local::now();

    let mut paths = config.includes.clone();
    let mut files = Vec::new();
    let mut stats = FileStats::default();

    while let Some(path) = paths.pop() {
        let path = match path.canonicalize() {
            Ok(path) => path,
            Err(err) => {
                warn!(
                    "Skipping path {} as it appears to be a broken symbolic link: {}",
                    path.display(),
                    err
                );
                continue;
            }
        };

        if let Some(exclude) = config
            .excludes
            .iter()
            .find(|exclude| path.starts_with(exclude))
        {
            info!(
                "Skipping path {} due to exclude {}",
                path.display(),
                exclude.display(),
            );
            continue;
        }

        if path.is_file() {
            files.push(path);

            if files.len() == 1000 {
                upload_files(&config, &srv_ip, &dev_id, &mut stats, &files)
                    .context("Failed to upload files")?;
                files.clear();
            }
        } else if path.is_dir() {
            let dir = match path.read_dir() {
                Ok(dir) => dir,
                Err(err) => {
                    warn!(
                        "Skipping directory {} as it appears to have been removed: {}",
                        path.display(),
                        err
                    );
                    continue;
                }
            };

            for entry in dir {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(err) => {
                        warn!(
                            "Skipping entry in directory {} as it appears to have been removed: {}",
                            path.display(),
                            err
                        );
                        continue;
                    }
                };

                paths.push(entry.path());
            }
        } else {
            warn!(
                "Skipping path {} as it is neither a file nor a directory",
                path.display()
            );
            continue;
        }
    }

    if !files.is_empty() {
        upload_files(&config, &srv_ip, &dev_id, &mut stats, &files)
            .context("Failed to upload files")?;
    }

    let endtime = Local::now();

    if stats.failed_to_backup != 0 {
        error!(
            "Failed to backup {} out of {} files",
            stats.failed_to_backup, stats.considered_for_backup
        );
    } else {
        info!("Finished backup of {} files", stats.considered_for_backup);
    }

    mail_summary(&config, &srv_ip, &starttime, &endtime, &stats)
        .context("Failed to mail summary")?;

    Ok(())
}

#[derive(Deserialize)]
struct Config {
    username: String,
    password: String,
    encryption_key: String,
    device_name: String,
    notify_email: String,
    includes: Vec<PathBuf>,
    excludes: Vec<PathBuf>,
}

fn read_config() -> Result<Config, Error> {
    let config_file = File::open("config.yaml")?;
    let config = from_yaml_reader(config_file)?;

    Ok(config)
}

fn download_util() -> Result<(), Error> {
    if Path::new("idevsutil_dedup").exists() {
        return Ok(());
    }

    warn!("Downloading idevsutil_dedup...");

    let status = Command::new("curl")
        .arg("-o")
        .arg("IDrive_linux_64bit.zip")
        .arg("https://www.idrivedownloads.com/downloads/linux/download-options/IDrive_linux_64bit.zip")
        .status()?;

    if !status.success() {
        bail!("Failed to download idevsutil_dedup using curl");
    }

    let status = Command::new("unzip")
        .arg("-j")
        .arg("IDrive_linux_64bit.zip")
        .arg("IDrive_linux_64bit/idevsutil_dedup")
        .status()?;

    if !status.success() {
        bail!("Failed to extract idevsutil_dedup using unzip");
    }

    remove_file("IDrive_linux_64bit.zip")?;
    set_permissions("idevsutil_dedup", Permissions::from_mode(0o755))?;

    Ok(())
}

fn run_util<I, S>(config: &Config, args: I) -> Result<String, Error>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let pass_file = NamedTempFile::new()?;
    write(&pass_file, &config.password)?;

    let key_file = NamedTempFile::new()?;
    write(&key_file, &config.encryption_key)?;

    let temp_dir = TempDir::new()?;

    let output = Command::new("./idevsutil_dedup")
        .arg(&make_arg("--password-file=", pass_file.path()))
        .arg(&make_arg("--pvt-key=", key_file.path()))
        .arg(&make_arg("--temp=", temp_dir.path()))
        .args(args)
        .env("LC_ALL", "C")
        .output()?;

    if !output.status.success() {
        match output.status.code() {
            Some(code) => bail!("idevsutil_dedup failed with exit code {}", code),
            None => bail!("idevsutil_dedup was terminated by signal"),
        }
    }

    Ok(String::from_utf8(output.stdout)?)
}

fn parse_tree<T: DeserializeOwned>(output: String) -> Result<T, Error> {
    let tree = if let Some(pos) = output.find("<tree") {
        &output[pos..]
    } else {
        bail!("Did not find expected tree in output");
    };

    Ok(from_xml_str(tree).map_err(SyncFailure::new)?)
}

fn parse_items<T: DeserializeOwned>(output: String) -> Result<Vec<T>, Error> {
    let mut items = Vec::new();

    for line in output.lines() {
        if line.starts_with("<item") {
            items.push(from_xml_str(&line).map_err(SyncFailure::new)?);
        }
    }

    Ok(items)
}

fn get_server_ip(config: &Config) -> Result<String, Error> {
    let output = run_util(&config, &["--getServerAddress", &config.username])?;

    #[derive(Deserialize)]
    #[serde(rename = "tree")]
    struct ServerIP {
        #[serde(rename = "cmdUtilityServerIP")]
        val: String,
    }

    let srv_ip = parse_tree::<ServerIP>(output)?;

    Ok(srv_ip.val)
}

fn get_device_id(config: &Config, srv_ip: &str) -> Result<String, Error> {
    let output = run_util(
        &config,
        &[
            "--list-device",
            &format!("{}@{}::home/", config.username, srv_ip),
        ],
    )?;

    #[derive(Deserialize)]
    #[serde(rename = "item")]
    struct Device {
        device_id: String,
        nick_name: String,
    }

    let devices = parse_items::<Device>(output)?;

    for device in devices {
        if device.nick_name == config.device_name {
            return Ok(format!("5c0b{}4b5z", device.device_id));
        }
    }

    bail!("Failed to resolve device ID");
}

fn get_quota(config: &Config, srv_ip: &str) -> Result<(u64, u64), Error> {
    let output = run_util(
        &config,
        &[
            "--get-quota",
            &format!("{}@{}::home/", config.username, srv_ip),
        ],
    )?;

    #[derive(Deserialize)]
    #[serde(rename = "tree")]
    struct Quota {
        #[serde(rename = "usedquota")]
        used: u64,
        #[serde(rename = "totalquota")]
        total: u64,
    }

    let quota = parse_tree::<Quota>(output)?;

    Ok((quota.used, quota.total))
}

#[derive(Default)]
struct FileStats {
    considered_for_backup: usize,
    backed_up_now: usize,
    already_present: usize,
    failed_to_backup: usize,
}

#[allow(clippy::borrowed_box)]
fn upload_files<I, P>(
    config: &Config,
    srv_ip: &str,
    dev_id: &str,
    stats: &mut FileStats,
    files: I,
) -> Result<(), Error>
where
    I: IntoIterator<Item = P>,
    P: AsRef<Path>,
{
    let list_file = NamedTempFile::new()?;
    let mut file_cnt = 0;

    {
        let mut list_file = BufWriter::new(list_file.as_file());

        for file in files {
            list_file.write_all(file.as_ref().as_os_str().as_bytes())?;
            list_file.write_all(b"\n")?;

            file_cnt += 1;
        }
    }

    info!("Uploading batch of {} files...", file_cnt);

    let output = run_util(
        &config,
        &[
            OsStr::from_bytes(b"--xml-output"),
            OsStr::from_bytes(b"--type"),
            &make_arg("--device-id=", dev_id),
            &make_arg("--files-from=", list_file.path()),
            OsStr::from_bytes(b"--relative"),
            OsStr::from_bytes(b"/"),
            OsStr::from_bytes(format!("{}@{}::home/", config.username, srv_ip).as_bytes()),
        ],
    )?;

    #[derive(Deserialize)]
    #[serde(rename = "item")]
    struct Transfer {
        #[serde(rename = "per")]
        percentage: String,
        #[serde(rename = "fname")]
        file_name: String,
        #[serde(rename = "trf_type")]
        type_: String,
        #[serde(rename = "rate_trf")]
        rate: String,
        #[serde(rename = "tottrf_sz")]
        total_size: u64,
    }

    let mut last_total_transfer_size = 0;

    let transfers = parse_items::<Transfer>(output)?;

    for transfer in transfers {
        if transfer.percentage != "100%" {
            continue;
        }

        let transfer_size = transfer.total_size - last_total_transfer_size;
        last_total_transfer_size = transfer.total_size;

        stats.considered_for_backup += 1;

        if transfer.type_ == "FULL" || transfer.type_ == "INCREMENTAL" {
            let (size, unit) = format_size(transfer_size);
            info!(
                "Transferred {:.1} {} at {} to backup file /{}",
                size, unit, transfer.rate, transfer.file_name
            );

            stats.backed_up_now += 1
        } else if transfer.type_ == "FILE IN SYNC" {
            stats.already_present += 1
        } else {
            error!(
                "Failed to backup file {} due to: {}",
                transfer.file_name, transfer.type_
            );

            stats.failed_to_backup += 1
        }
    }

    Ok(())
}

fn mail_summary(
    config: &Config,
    srv_ip: &str,
    starttime: &DateTime<Local>,
    endtime: &DateTime<Local>,
    stats: &FileStats,
) -> Result<(), Error> {
    let (quota_used, quota_total) = get_quota(config, srv_ip).context("Failed to get quota")?;

    let summary = format!(
        r#"
Summary:
Machine: {device_name} ({hostname})
Backup start time: {starttime}
Backup end time: {endtime}
Files considered for backup: {files_considered_for_backup}
Files backed up now: {files_backed_up_now}
Files already present in your account: {files_already_present}
Files failed to backup: {files_failed_to_backup}
Quota used: {quota_used} GB out of {quota_total} GB"#,
        device_name = config.device_name,
        hostname = get_hostname()?,
        starttime = starttime,
        endtime = endtime,
        files_considered_for_backup = stats.considered_for_backup,
        files_backed_up_now = stats.backed_up_now,
        files_already_present = stats.already_present,
        files_failed_to_backup = stats.failed_to_backup,
        quota_used = quota_used >> 30,
        quota_total = quota_total >> 30,
    );

    let subject = if stats.failed_to_backup != 0 {
        format!(
            "Incomplete backup summary ({} out of {})",
            stats.failed_to_backup, stats.considered_for_backup
        )
    } else {
        "Successful backup summary".to_owned()
    };

    let status = Command::new("curl")
        .arg("--silent")
        .arg("--data-urlencode")
        .arg(&format!("username={}", config.username))
        .arg("--data-urlencode")
        .arg(&format!("password={}", config.password))
        .arg("--data-urlencode")
        .arg(&format!("to_email={}", config.notify_email))
        .arg("--data-urlencode")
        .arg(&format!("content={}", summary))
        .arg("--data-urlencode")
        .arg(&format!("subject={}", subject))
        .arg("https://webdav.ibackup.com/cgi-bin/Notify_email_ibl")
        .stdout(Stdio::null())
        .status()?;

    if !status.success() {
        warn!("Could not send summary via electronic mail using curl");
    }

    Ok(())
}

fn make_arg<S: AsRef<OsStr>>(pre: &str, val: S) -> OsString {
    let mut arg = OsString::new();
    arg.push(pre);
    arg.push(val);
    arg
}

#[allow(clippy::useless_let_if_seq)]
fn format_size(size: u64) -> (f64, &'static str) {
    let mut size = size as f64;
    let mut unit = "B";

    if size > 1024.0 {
        size /= 1024.0;
        unit = "kB";
    }

    if size > 1024.0 {
        size /= 1024.0;
        unit = "MB";
    }

    if size > 1024.0 {
        size /= 1024.0;
        unit = "GB";
    }

    (size, unit)
}

fn get_hostname() -> Result<String, Error> {
    let mut hostname = String::from_utf8(Command::new("hostname").output()?.stdout)?;
    hostname.pop();
    Ok(hostname)
}
