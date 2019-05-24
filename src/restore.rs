use std::ffi::{OsStr, OsString};
use std::io::{BufWriter, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use failure::Fallible;
use log::info;
use serde_derive::Deserialize;
use tempfile::NamedTempFile;

use super::{format_size, list_dir, make_arg, parse_items, run_util, Config};

pub fn restore(config: &Config, srv_ip: &str, dev_id: &str, dir: &Path) -> Fallible<()> {
    info!(
        "Restoring backup of {} ({}) from {}...",
        config.device_name, dev_id, srv_ip
    );

    let list_file = NamedTempFile::new()?;

    {
        let mut list_file = BufWriter::new(list_file.as_file());

        for (entry, _) in list_dir(config, srv_ip, dev_id, Path::new("/"))? {
            list_file.write_all(entry.as_os_str().as_bytes())?;
            list_file.write_all(b"\n")?;
        }
    }

    let output = run_util(
        &config,
        &[
            OsStr::new("--xml-output"),
            &make_arg("--files-from=", list_file.path()),
            &make_arg("--device-id=", dev_id),
            &OsString::from(format!("{}@{}::home/", config.username, srv_ip)),
            dir.as_os_str(),
        ],
    )?;

    #[derive(Deserialize)]
    #[serde(rename = "item")]
    struct Transfer {
        #[serde(rename = "tottrf_sz")]
        total_size: u64,
    }

    let transfers = parse_items::<Transfer>(output)?;

    let total_transfer_size = transfers
        .iter()
        .map(|transfer| transfer.total_size)
        .max()
        .unwrap_or(0);

    let (size, unit) = format_size(total_transfer_size);

    info!("Transferred {:.1} {} during restore.", size, unit);

    Ok(())
}
