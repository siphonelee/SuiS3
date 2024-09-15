use std::process::Command;
use anyhow::bail;
use std::fs;
use serde::{Serialize, Deserialize};
use serde_with::{DisplayFromStr, serde_as};

#[serde_as]
#[derive(Deserialize, Serialize, Debug)]
pub struct BlobMeta {
    #[serde_as(as = "DisplayFromStr")]
    pub size: u64,
    pub tags: Vec<String>,
    #[serde_as(as = "DisplayFromStr")]
    pub last_write_ts: u64,
    pub walrus_blob_id: String,
    #[serde_as(as = "DisplayFromStr")]
    pub walrus_epoch_till: u64,
}

pub fn walrus_blob_status(blob_id: &str) -> Result<u64, anyhow::Error> {
    let output = Command::new("walrus")
        .arg("blob-status")
        .arg("--blob-id")
        .arg(blob_id)
        .output()?;

    if !&output.status.success() {
        bail!("{}", String::from_utf8_lossy(&output.stderr));
    } else {
        let console_output = String::from_utf8_lossy(&output.stdout);
        let split = console_output.split("\n");        

        let mut end_epoch = 0;
        for part in split {
            if part.starts_with("End epoch:") {
                end_epoch = part.split(":").last().unwrap().trim().parse::<u64>().unwrap();
            }
        }
        if end_epoch == 0 {
            // incorrect end epoch
            bail!("end epoch not found");
        }

        Ok(end_epoch)
    }
}

pub fn walrus_upload_file(filename: &String) -> Result<BlobMeta, anyhow::Error> {
    let len = fs::metadata(filename.clone())?.len();

    let output = Command::new("walrus")
        .arg("store")
        .arg(filename)
        .output()?;

    if !&output.status.success() {
        bail!("{}", String::from_utf8_lossy(&output.stderr));
    } else {
        let console_output = String::from_utf8_lossy(&output.stdout);
        let split = console_output.split("\n");
        let mut blob_id = "";
        let mut end_epoch = 0;
        for part in split {
            if part.starts_with("Blob ID:") {
                blob_id = part.split(":").last().unwrap().trim();
            } else if part.starts_with("End epoch:") {
                end_epoch = part.split(":").last().unwrap().trim().parse::<u64>().unwrap();
            }
        }

        if end_epoch == 0 && blob_id.len() > 0 {
            // end epoch info not included. run blob status
            end_epoch = walrus_blob_status(blob_id)?;
        }

        if end_epoch == 0 || blob_id.len() == 0 {
            bail!("no blob id found or incorrect end epoch");
        }

        let m = BlobMeta {
            size: len,
            walrus_blob_id: blob_id.to_owned(),
            walrus_epoch_till: end_epoch,
            tags: Vec::<String>::new(),
            last_write_ts: 0,
        };

        Ok(m)
    }
}

pub fn walrus_download_file(blob_id: &String, dest_file: &String) -> Result<(), anyhow::Error> {
    let output = Command::new("walrus")
        .arg("read")
        .arg(blob_id)
        .arg("--out")
        .arg(dest_file)
        .output()?;

    if !&output.status.success() {
        bail!("{}", String::from_utf8_lossy(&output.stderr));
    } else {
        Ok(())
    }
}