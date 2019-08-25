mod backup;
mod clean;
mod restore;

use std::env::args;
use std::ffi::{OsStr, OsString};
use std::fs::{remove_file, set_permissions, write, File, Permissions};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use failure::{bail, Fallible, ResultExt, SyncFailure};
use log::warn;
use serde::de::DeserializeOwned;
use serde_derive::Deserialize;
use serde_xml_rs::from_str as from_xml_str;
use serde_yaml::from_reader as from_yaml_reader;
use simplelog::{ConfigBuilder as LogConfigBuilder, LevelFilter, SimpleLogger};
use tempfile::{NamedTempFile, TempDir};

use self::backup::backup;
use self::clean::clean;
use self::restore::restore;

fn main() -> Fallible<()> {
    SimpleLogger::init(
        LevelFilter::Info,
        LogConfigBuilder::new()
            .set_time_level(LevelFilter::Off)
            .set_target_level(LevelFilter::Off)
            .build(),
    )
    .unwrap();

    download_util().context("Failed to download idevsutil_dedup")?;

    let config = read_config().context("Failed to read config")?;
    let srv_ip = get_server_ip(&config).context("Failed to determine server IP")?;
    let dev_id = get_device_id(&config, &srv_ip).context("Failed to determine device ID")?;

    let args = args().collect::<Vec<_>>();

    match args.get(1).map(String::as_str) {
        None | Some("backup") => backup(&config, &srv_ip, &dev_id),
        Some("restore") => {
            let dir = Path::new(args.get(2).map(String::as_str).unwrap_or("restored_files"));

            restore(&config, &srv_ip, &dev_id, dir)
        }
        Some("clean") => clean(&config, &srv_ip, &dev_id),
        Some(arg) => bail!("Unsupported mode: {}", arg),
    }
}

#[derive(Deserialize)]
pub struct Config {
    username: String,
    password: String,
    encryption_key: String,
    device_name: String,
    notify_email: String,
    includes: Vec<PathBuf>,
    excludes: Vec<PathBuf>,
}

fn read_config() -> Fallible<Config> {
    let config_file = File::open("config.yaml")?;
    let config = from_yaml_reader(config_file)?;

    Ok(config)
}

fn download_util() -> Fallible<()> {
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

fn run_util<I, S>(config: &Config, args: I) -> Fallible<String>
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

fn parse_tree<T: DeserializeOwned>(output: String) -> Fallible<T> {
    let tree = if let Some(pos) = output.find("<tree") {
        &output[pos..]
    } else {
        bail!("Did not find expected tree in output");
    };

    Ok(from_xml_str(tree).map_err(SyncFailure::new)?)
}

fn parse_items<T: DeserializeOwned>(output: String) -> Fallible<Vec<T>> {
    let mut items = Vec::new();

    for line in output.lines() {
        if line.starts_with("<item") {
            items.push(from_xml_str(&line).map_err(SyncFailure::new)?);
        }
    }

    Ok(items)
}

fn get_server_ip(config: &Config) -> Fallible<String> {
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

fn get_device_id(config: &Config, srv_ip: &str) -> Fallible<String> {
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

fn get_quota(config: &Config, srv_ip: &str) -> Fallible<(u64, u64)> {
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

fn list_dir(
    config: &Config,
    srv_ip: &str,
    dev_id: &str,
    dir: &Path,
) -> Fallible<impl Iterator<Item = (PathBuf, bool)>> {
    let output = run_util(
        &config,
        &[
            OsStr::new("--auth-list"),
            OsStr::new("--xml-output"),
            &make_arg("--device-id=", dev_id),
            &make_arg(&format!("{}@{}::home", config.username, srv_ip), dir),
        ],
    )?;

    #[derive(Deserialize)]
    #[serde(rename = "item")]
    struct Resource {
        #[serde(rename = "restype")]
        type_: char,
        #[serde(rename = "fname")]
        name: PathBuf,
    }

    let resources = parse_items::<Resource>(output)?;

    Ok(resources
        .into_iter()
        .filter_map(|resource| match resource.type_ {
            'D' => Some((resource.name, true)),
            'F' => Some((resource.name, false)),
            type_ => {
                warn!("Skipping unknown resource type: {}", type_);

                None
            }
        }))
}

fn make_arg<S: AsRef<OsStr>>(pre: &str, val: S) -> OsString {
    let mut arg = OsString::new();
    arg.push(pre);
    arg.push(val);
    arg
}

fn get_hostname() -> Fallible<String> {
    let mut hostname = String::from_utf8(Command::new("hostname").output()?.stdout)?;
    hostname.pop();
    Ok(hostname)
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
