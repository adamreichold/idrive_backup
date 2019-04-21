use std::ffi::{OsStr, OsString};
use std::io::{BufWriter, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use failure::{bail, Error, ResultExt};
use log::{info, warn};
use serde_derive::Deserialize;
use tempfile::NamedTempFile;

use super::{make_arg, parse_items, run_util, Config};

pub fn clean(config: &Config, srv_ip: &str, dev_id: &str) -> Result<(), Error> {
    info!(
        "Cleaning archive of {} ({}) at {}...",
        config.device_name, dev_id, srv_ip
    );

    let mut paths = vec![PathBuf::from("/")];
    let mut items = Vec::new();

    while let Some(path) = paths.pop() {
        for entry in list_folder(config, srv_ip, dev_id, &path)? {
            let entry_path = path.join(entry.name());

            if !test_path(config, &entry_path) {
                items.push(entry_path);

                if items.len() == 100 {
                    delete_items(config, srv_ip, dev_id, &items)
                        .context("Failed to delete items")?;
                    items.clear()
                }

                continue;
            }

            if let Entry::Dir(_) = entry {
                paths.push(entry_path);
            }
        }
    }

    if !items.is_empty() {
        delete_items(config, srv_ip, dev_id, &items).context("Failed to delete items")?;
    }

    Ok(())
}

#[derive(Debug)]
enum Entry {
    Dir(PathBuf),
    File(PathBuf),
}

impl Entry {
    fn name(&self) -> &Path {
        match self {
            Entry::Dir(name) => name,
            Entry::File(name) => name,
        }
    }
}

fn test_path(config: &Config, path: &Path) -> bool {
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

fn list_folder(
    config: &Config,
    srv_ip: &str,
    dev_id: &str,
    path: &Path,
) -> Result<Vec<Entry>, Error> {
    let output = run_util(
        &config,
        &[
            OsStr::new("--auth-list"),
            OsStr::new("--xml-output"),
            &make_arg("--device-id=", dev_id),
            &make_arg(&format!("{}@{}::home", config.username, srv_ip), path),
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
            'D' => Some(Entry::Dir(resource.name)),
            'F' => Some(Entry::File(resource.name)),
            type_ => {
                warn!("Skipping unknown resource type: {}", type_);

                None
            }
        })
        .collect())
}

fn delete_items<I, P>(config: &Config, srv_ip: &str, dev_id: &str, items: I) -> Result<(), Error>
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
