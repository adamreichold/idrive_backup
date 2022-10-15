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
use std::ffi::{OsStr, OsString};
use std::io::{BufWriter, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use tempfile::NamedTempFile;

use super::{context, format_size, make_arg, parse_items, run_util, walk_dir, Config, Fallible};

pub fn restore_missing(
    config: &Config,
    srv_ip: &str,
    dev_id: &str,
    sub_dir: &Path,
    out_dir: &Path,
) -> Fallible {
    eprintln!(
        "Restoring missing files from backup of {} ({}) from {}...",
        config.device_name, dev_id, srv_ip
    );

    let mut items = Vec::new();

    walk_dir(config, srv_ip, dev_id, sub_dir, |path| {
        if path.canonicalize().is_err() {
            eprintln!("Restoring item {} from archive", path.display());

            items.push(path.clone());

            if items.len() == 100 {
                restore_items(config, srv_ip, dev_id, out_dir, &items)
                    .map_err(context("Failed to delete items"))?;

                items.clear();
            }
        }

        Ok(Some(path))
    })?;

    if !items.is_empty() {
        restore_items(config, srv_ip, dev_id, out_dir, &items)
            .map_err(context("Failed to delete items"))?;
    }

    Ok(())
}

fn restore_items(
    config: &Config,
    srv_ip: &str,
    dev_id: &str,
    dir: &Path,
    items: &[PathBuf],
) -> Fallible {
    let list_file = NamedTempFile::new()?;

    {
        let mut list_file = BufWriter::new(list_file.as_file());

        for item in items {
            list_file.write_all(item.as_os_str().as_bytes())?;
            list_file.write_all(b"\n")?;
        }
    }

    let output = run_util(
        config,
        [
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

    eprintln!("Transferred {:.1} {} during restore.", size, unit);

    Ok(())
}
