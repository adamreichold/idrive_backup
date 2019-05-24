use std::ffi::{OsStr, OsString};
use std::io::{BufWriter, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::process::{Command, Stdio};

use chrono::{offset::Local, DateTime};
use failure::{Fallible, ResultExt};
use log::{error, info, warn};
use serde_derive::Deserialize;
use tempfile::NamedTempFile;

use super::{format_size, get_hostname, get_quota, make_arg, parse_items, run_util, Config};

pub fn backup(config: &Config, srv_ip: &str, dev_id: &str) -> Fallible<()> {
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
    let mut stats = Stats::default();

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
                upload_files(config, srv_ip, dev_id, &mut stats, &files)
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
        upload_files(config, srv_ip, dev_id, &mut stats, &files)
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

#[derive(Default)]
struct Stats {
    considered_for_backup: usize,
    backed_up_now: usize,
    already_present: usize,
    failed_to_backup: usize,
}

fn upload_files<I, P>(
    config: &Config,
    srv_ip: &str,
    dev_id: &str,
    stats: &mut Stats,
    files: I,
) -> Fallible<()>
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
            OsStr::new("--xml-output"),
            OsStr::new("--type"),
            &make_arg("--device-id=", dev_id),
            &make_arg("--files-from=", list_file.path()),
            OsStr::new("--relative"),
            OsStr::new("/"),
            &OsString::from(format!("{}@{}::home/", config.username, srv_ip)),
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
    stats: &Stats,
) -> Fallible<()> {
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