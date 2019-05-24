use std::ffi::{OsStr, OsString};
use std::io::{BufWriter, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use failure::{bail, Fallible, ResultExt};
use log::{info, warn};
use serde_derive::Deserialize;
use tempfile::NamedTempFile;

use super::{list_dir, make_arg, parse_items, run_util, Config};

pub fn clean(config: &Config, srv_ip: &str, dev_id: &str) -> Fallible<()> {
    info!(
        "Cleaning archive of {} ({}) at {}...",
        config.device_name, dev_id, srv_ip
    );

    let mut items = Vec::new();

    walk_dir(config, srv_ip, dev_id, Path::new("/"), |path| {
        if exists_and_not_excluded(config, &path) {
            Ok(Some(path))
        } else {
            items.push(path);

            if items.len() == 100 {
                delete_items(config, srv_ip, dev_id, &items).context("Failed to delete items")?;

                items.clear()
            }

            Ok(None)
        }
    })?;

    if !items.is_empty() {
        delete_items(config, srv_ip, dev_id, &items).context("Failed to delete items")?;
    }

    Ok(())
}

fn exists_and_not_excluded(config: &Config, path: &Path) -> bool {
    let path = match path.canonicalize() {
        Ok(path) => path,
        Err(_) => return false,
    };

    if config
        .excludes
        .iter()
        .any(|exclude| path.starts_with(exclude))
    {
        return false;
    }

    true
}

fn walk_dir<F: FnMut(PathBuf) -> Fallible<Option<PathBuf>>>(
    config: &Config,
    srv_ip: &str,
    dev_id: &str,
    dir: &Path,
    mut f: F,
) -> Fallible<()> {
    let mut dirs = vec![dir.to_path_buf()];

    while let Some(dir) = dirs.pop() {
        for (entry, is_dir) in list_dir(config, srv_ip, dev_id, &dir)? {
            let path = dir.join(entry);

            if let Some(path) = f(path)? {
                if is_dir {
                    dirs.push(path);
                }
            }
        }
    }

    Ok(())
}

fn delete_items<I, P>(config: &Config, srv_ip: &str, dev_id: &str, items: I) -> Fallible<()>
where
    I: IntoIterator<Item = P>,
    P: AsRef<Path>,
{
    let list_file = NamedTempFile::new()?;
    let mut item_cnt = 0;

    {
        let mut list_file = BufWriter::new(list_file.as_file());

        for item in items {
            warn!("Deleting item {} from archive", item.as_ref().display());

            list_file.write_all(item.as_ref().as_os_str().as_bytes())?;
            list_file.write_all(b"\n")?;

            item_cnt += 1;
        }
    }

    let output = run_util(
        &config,
        &[
            OsStr::new("--delete-items"),
            OsStr::new("--xml-output"),
            &make_arg("--files-from=", list_file.path()),
            OsStr::new("--relative"),
            &make_arg("--device-id=", dev_id),
            &OsString::from(format!("{}@{}::home/", config.username, srv_ip)),
        ],
    )?;

    #[derive(Deserialize)]
    #[serde(rename = "item")]
    struct Operation {
        #[serde(rename = "tot_items_deleted")]
        items_deleted: Option<usize>,
    }

    let operations = parse_items::<Operation>(output)?;

    for operation in operations {
        if let Some(items_deleted) = operation.items_deleted {
            if items_deleted == item_cnt {
                return Ok(());
            } else {
                bail!("Deleted only {} of {} items", items_deleted, item_cnt);
            }
        }
    }

    bail!("Deletion of {} items was not confirmed", item_cnt);
}
