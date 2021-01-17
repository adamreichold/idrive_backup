/*
Copyright 2019 Adam Reichold

This file is part of b2_backup.

b2_backup is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

b2_backup is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with b2_backup.  If not, see <https://www.gnu.org/licenses/>.
*/
mod backup;
mod clean;
mod restore;

use std::env::args;
use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fs::{remove_file, set_permissions, write, File, Permissions};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use quick_xml::de::from_str as from_xml_str;
use serde::{de::DeserializeOwned, Deserialize};
use serde_yaml::from_reader as from_yaml_reader;
use tempfile::{NamedTempFile, TempDir};

use self::backup::backup;
use self::clean::clean;
use self::restore::restore;

type Fallible<T = ()> = Result<T, Box<dyn Error>>;

fn context(msg: &'static str) -> impl FnOnce(Box<dyn Error>) -> Box<dyn Error> {
    move |err| format!("{}: {}", msg, err).into()
}

fn main() -> Fallible {
    download_util().map_err(context("Failed to download idevsutil_dedup"))?;

    let config = read_config().map_err(context("Failed to read config"))?;
    let srv_ip = get_server_ip(&config).map_err(context("Failed to determine server IP"))?;
    let dev_id =
        get_device_id(&config, &srv_ip).map_err(context("Failed to determine device ID"))?;

    let args = args().collect::<Vec<_>>();

    match args.get(1).map(String::as_str) {
        None | Some("backup") => backup(&config, &srv_ip, &dev_id),
        Some("restore") => {
            let dir = Path::new(args.get(2).map(String::as_str).unwrap_or("restored_files"));

            restore(&config, &srv_ip, &dev_id, dir)
        }
        Some("clean") => clean(&config, &srv_ip, &dev_id),
        Some(arg) => Err(format!("Unsupported mode: {}", arg).into()),
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

fn download_util() -> Fallible {
    if Path::new("idevsutil_dedup").exists() {
        return Ok(());
    }

    eprintln!("Downloading idevsutil_dedup...");

    let status = Command::new("curl")
        .arg("-o")
        .arg("IDrive_linux_64bit.zip")
        .arg("https://www.idrivedownloads.com/downloads/linux/download-options/IDrive_linux_64bit.zip")
        .status()?;

    if !status.success() {
        return Err("Failed to download idevsutil_dedup using curl".into());
    }

    let status = Command::new("unzip")
        .arg("-j")
        .arg("IDrive_linux_64bit.zip")
        .arg("IDrive_linux_64bit/idevsutil_dedup")
        .status()?;

    if !status.success() {
        return Err("Failed to extract idevsutil_dedup using unzip".into());
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
        return Err(format!("idevsutil_dedup failed with status {:?}", output.status).into());
    }

    Ok(String::from_utf8(output.stdout)?)
}

fn parse_tree<T: DeserializeOwned>(output: String) -> Fallible<T> {
    let tree = if let Some(pos) = output.find("<tree") {
        &output[pos..]
    } else {
        return Err("Did not find expected tree in output".into());
    };

    from_xml_str(tree).map_err(Into::into)
}

fn parse_items<T: DeserializeOwned>(output: String) -> Fallible<Vec<T>> {
    let mut items = Vec::new();

    for line in output.lines() {
        if line.starts_with("<item") {
            items.push(from_xml_str(&line)?);
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

    Err("Failed to resolve device ID".into())
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
                eprintln!("Skipping unknown resource type: {}", type_);

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
