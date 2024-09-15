use regex::Regex;
#[cfg(feature = "async")]

use clap::{Parser, ValueEnum};
use chrono::prelude::*;
use std::path::Path;
use std::env;
use std::fs;

use super::operations;
use super::walrus;

static SUIS3_REGEXP: &str = r#"[sS][uU][iI][sS]3:\/\/(?P<bucket>[A-Za-z0-9\-\._]+)(?P<object>[A-Za-z0-9\-\._\/]*)"#;

#[derive(Parser, Debug)]
#[command(name = "suis3")]
pub struct Cli {
    #[command(subcommand)]
    pub suis3_cmd: Option<SuiS3Cmd>,
}

#[derive(Parser, PartialEq, Debug)]
pub enum SuiS3Cmd {
    #[command(name = "la", about = "list all buckets")]
    ListAll,

    #[command(
        name = "ls",
        about = r#"list all buckets, or list all objects of a bucket
    ls s3://<bucket>"#
    )]
    List { uri: Option<String> },

    #[command(
        name = "ll",
        about = r#"list all objects detail of the bucket
    ll s3://<bucket>"#
    )]
    Detail { uri: Option<String> },

    #[command(
        name = "mb",
        about = r#"create bucket
    mb s3://<bucket>"#
    )]
    CreateBucket { bucket: String },

    #[command(
        name = "rb",
        about = r#"delete bucket
    rb s3://<bucket>"#
    )]
    DeleteBucket { bucket: String },

    #[command(about = r#"upload the file with specify object name
    put <file> s3://<bucket>/<object>
upload the file as the same file name
    put <file> s3://<bucket>"#)]
    Put { file: String, uri: String },

    #[command(about = r#"download the object
    get s3://<bucket>/<object> <file>
download the object to current folder
    get s3://<bucket>/<object>"#)]
    Get { uri: String, file: Option<String> },

    #[command(about = r#"display the object content
    cat s3://<bucket>/<object>"#)]
    Cat { uri: String },

    #[command(about = r#"delete the object
    del s3://<bucket>/<object>"#)]
    Del { uri: String },

    #[command(about = r#"delete the object
    rm s3://<bucket>/<object>"#)]
    Rm { uri: String },

    #[command(about = r#"tag operations
list tags of the bucket or the object
    tag ls/list s3://<bucket>[/<object>]
add tags to the object
    tag add/put s3://<bucket>/<object>  <key>=<value> ...
remove tags from the object
    tag del/rm s3://<bucket>/<object>"#)]
    Tag {
        #[arg(value_enum)]
        action: TagAction,
        uri: String,
        tags: Vec<String>,
    },

    #[command(name = "quit/exit", about = "quit the programe")]
    Quit,
    // #[command(name = "help", about = "show s3 command usage")]
    // Help,
}

#[derive(ValueEnum, PartialEq, Debug, Clone)]
pub enum TagAction {
    List,
    Ls,
    Add,
    Put,
    Del,
    Rm,
}

pub async fn do_command(command: Option<SuiS3Cmd>) {
    // println!("===== do command: {:?} =====", command);
    match command {
        Some(SuiS3Cmd::ListAll) => {
            match operations::list_buckets().await {
                Err(e) => println!("{}", e),
                Ok(v) => {
                    println!("TIME\t\t\t\tBUCKET NAME");
                    for bi in v.iter() {
                        let timestamp = NaiveDateTime::from_timestamp((bi.create_ts/1000) as i64, 0);
                        let date_time: DateTime<Local> = Local.from_local_datetime(&timestamp).unwrap();
                        println!("{}\t{}", date_time, bi.name);
                    }
                }
            }
        },
        Some(SuiS3Cmd::CreateBucket { bucket }) => {
            let re = Regex::new(SUIS3_REGEXP).unwrap();
            let caps = re.captures(&bucket);
            if caps.is_none() {
                println!("SUIS3 object format error.");
                return;
            }
            let caps = caps.unwrap();
            let name = &caps["bucket"];

            match operations::create_bucket(name.to_owned()).await {
                Err(e) => println!("{}", e),
                Ok(()) => {}
            }
        }
        Some(SuiS3Cmd::DeleteBucket { bucket }) => {
            let re = Regex::new(SUIS3_REGEXP).unwrap();
            let caps = re.captures(&bucket);
            if caps.is_none() {
                println!("SUIS3 object format error.");
                return;
            }
            let caps = caps.unwrap();
            let name = &caps["bucket"];

            match operations::delete_bucket(name.to_owned()).await {
                Err(e) => println!("{}", e),
                Ok(()) => {}
            }
        }
        Some(SuiS3Cmd::Tag {
            action: TagAction::Add,
            uri,
            tags,
        }) | Some(SuiS3Cmd::Tag {
            action: TagAction::Put,
            uri,
            tags,
        }) => {
            let re = Regex::new(SUIS3_REGEXP).unwrap();
            let caps = re.captures(&uri);
            if caps.is_none() {
                println!("SUIS3 object format error.");
                return;
            }
            let caps = caps.unwrap();        
            let bucket_name = &caps["bucket"];
            let obj_name = &caps["object"];

            if obj_name.len() == 0 {
                // tag bucket 
                match operations::tag_bucket(bucket_name.to_owned(), tags).await {
                    Err(e) => println!("{}", e),
                    Ok(()) => {}
                }    
            } else {
                match operations::tag_object(bucket_name.to_owned(), obj_name.to_owned(), tags).await {
                    Err(e) => println!("{}", e),
                    Ok(()) => {}
                }    
            }
        }

        Some(SuiS3Cmd::Tag {
            action: TagAction::List,
            uri,
            ..
        }) | Some(SuiS3Cmd::Tag {
            action: TagAction::Ls,
            uri,
            ..
        }) => {
            let re = Regex::new(SUIS3_REGEXP).unwrap();
            let caps = re.captures(&uri);
            if caps.is_none() {
                println!("SUIS3 object format error.");
                return;
            }
            let caps = caps.unwrap();        
            let bucket_name = &caps["bucket"];
            let obj_name = &caps["object"];

            if obj_name.len() == 0 {
                // list bucket tag 
                match operations::list_bucket_tags(bucket_name.to_owned()).await {
                    Err(e) => println!("{}", e),
                    Ok(v) => {
                        for s in v.iter() {
                            println!("{}", s);
                        }    
                    }
                }    
            } else {
                match operations::list_object_tags(bucket_name.to_owned(), obj_name.to_owned()).await {
                    Err(e) => println!("{}", e),
                    Ok(v) => {
                        for s in v.iter() {
                            println!("{}", s);
                        }    
                    }
                }                 
            }
        }
 
        Some(SuiS3Cmd::Tag {
            action: TagAction::Del,
            uri,
            ..
        }) | Some(SuiS3Cmd::Tag {
            action: TagAction::Rm,
            uri,
            ..
        }) => {
            let re = Regex::new(SUIS3_REGEXP).unwrap();
            let caps = re.captures(&uri);
            if caps.is_none() {
                println!("SUIS3 object format error.");
                return;
            }
            let caps = caps.unwrap();        
            let bucket_name = &caps["bucket"];
            let obj_name = &caps["object"];

            if obj_name.len() == 0 {
                // delete bucket tag                
                match operations::delete_bucket_tags(bucket_name.to_owned()).await {
                    Err(e) => println!("{}", e),
                    Ok(()) => {}
                }
            } else {
                match operations::delete_object_tags(bucket_name.to_owned(), obj_name.to_owned()).await {
                    Err(e) => println!("{}", e),
                    Ok(()) => {}
                }
            }
        }

        Some(SuiS3Cmd::Put { uri, file }) => {
            let re = Regex::new(SUIS3_REGEXP).unwrap();
            let caps = re.captures(&uri);
            if caps.is_none() {
                println!("SUIS3 object format error.");
                return;
            }
            let caps = caps.unwrap();        
            let bucket_name = &caps["bucket"];
            let mut obj_name = caps["object"].to_owned();

            if obj_name.len() == 0 || obj_name == "/" {
                let path = Path::new(&file);
                let filename = path.file_name().unwrap();
                obj_name = "/".to_owned() + filename.to_str().unwrap();
            } 

            match operations::put_object(bucket_name, obj_name.as_str(), &file).await {
                Err(e) => println!("{}", e),
                Ok(meta) => {
                    println!("Blob id: {}", meta.walrus_blob_id);
                }
            }
        }

        Some(SuiS3Cmd::Get { uri, file }) => {
            let re = Regex::new(SUIS3_REGEXP).unwrap();
            let caps = re.captures(&uri);
            if caps.is_none() {
                println!("SUIS3 object format error.");
                return;
            }
            let caps = caps.unwrap();        
            let bucket_name = &caps["bucket"];
            let obj_name = &caps["object"];

            if obj_name.len() == 0 {
                println!("SUIS3 object format error.");
            } else {
                let dest_filename;                
                if file.is_none() {
                    let path = Path::new(obj_name);
                    let filename = path.file_name().unwrap();
                    dest_filename = filename.to_str().unwrap().to_owned();    
                } else {
                    dest_filename = file.unwrap();
                }
                
                match operations::get_object_id(bucket_name.to_owned(), obj_name.to_owned()).await {
                    Err(e) => println!("{}", e),
                    Ok(blob_id) => {
                        match walrus::walrus_download_file(&blob_id, &dest_filename) {
                            Err(e) => println!("{}", e),
                            Ok(()) => {
                                println!("Saved as: {}", dest_filename);
                            }            
                        }
                    }
                }                
            }
        }

        Some(SuiS3Cmd::Cat { uri }) => {
            let re = Regex::new(SUIS3_REGEXP).unwrap();
            let caps = re.captures(&uri);
            if caps.is_none() {
                println!("SUIS3 object format error.");
                return;
            }
            let caps = caps.unwrap();        
            let bucket_name = &caps["bucket"];
            let obj_name = &caps["object"];

            if obj_name.len() == 0 {
                println!("SUIS3 object format error.");
            } else {
                let dir = env::temp_dir();
                let dest_filename = dir.to_str().unwrap().to_string() + "suis3_tmp";                

                match operations::get_object_id(bucket_name.to_owned(), obj_name.to_owned()).await {
                    Err(e) => println!("{}", e),
                    Ok(blob_id) => {
                        match walrus::walrus_download_file(&blob_id, &dest_filename) {
                            Err(e) => println!("{}", e),
                            Ok(()) => {
                                if let Ok(content) = fs::read_to_string(dest_filename.clone()) {
                                    println!("{}", content);
                                } 
                                let _ = fs::remove_file(dest_filename);
                            }            
                        }
                    }
                }                
            }
        }

        Some(SuiS3Cmd::Del { uri }) 
        | Some(SuiS3Cmd::Rm { uri }) => {
            let re = Regex::new(SUIS3_REGEXP).unwrap();
            let caps = re.captures(&uri);
            if caps.is_none() {
                println!("SUIS3 object format error.");
                return;
            }
            let caps = caps.unwrap();        
            let bucket_name = &caps["bucket"];
            let obj_name = &caps["object"];

            if obj_name.len() == 0 {
                println!("SUIS3 object format error.");
            } else {
                match operations::delete_object(bucket_name.to_owned(), obj_name.to_owned()).await {
                    Err(e) => println!("{}", e),
                    Ok(()) => {}
                }
            }
        }


        Some(SuiS3Cmd::List { uri }) => {
            if uri.is_none() {
                // list all buckets
                match operations::list_buckets().await {
                    Err(e) => println!("{}", e),
                    Ok(v) => {
                        println!("TIME\t\t\t\tBUCKET NAME");
                        for bi in v.iter() {
                            let timestamp = NaiveDateTime::from_timestamp((bi.create_ts/1000) as i64, 0);
                            let date_time: DateTime<Local> = Local.from_local_datetime(&timestamp).unwrap();
                            println!("{}\t{}", date_time, bi.name);
                        }
                    }
                }    
            } else {
                // list one bucket
                let uri = uri.unwrap();
                let re = Regex::new(SUIS3_REGEXP).unwrap();
                let caps = re.captures(&uri);
                if caps.is_none() {
                    println!("SUIS3 object format error.");
                    return;
                }
                let caps = caps.unwrap();                
                let bucket_name = &caps["bucket"];
                let obj_name = &caps["object"];
    
                if obj_name.len() != 0 {
                    println!("SUIS3 object format error.");
                } else {
                    match operations::get_bucket_objects(bucket_name.to_owned()).await {
                        Err(e) => println!("{}", e),
                        Ok(ret) => {
                            println!("URI\t\t\tTIME");
                            for obj in ret.objects.iter() {
                                let timestamp = NaiveDateTime::from_timestamp((obj.last_write_ts/1000) as i64, 0);
                                let date_time: DateTime<Local> = Local.from_local_datetime(&timestamp).unwrap();
                                println!("{}\t{}", obj.uri, date_time);                                                               
                            }
                        }
                    }                    
                }
            }
        },
        Some(SuiS3Cmd::Detail { uri }) => {
            if uri.is_none() {
                // list all buckets
                match operations::list_buckets().await {
                    Err(e) => println!("{}", e),
                    Ok(v) => {
                        println!("TIME\t\t\t\tBUCKET NAME");
                        for bi in v.iter() {
                            let timestamp = NaiveDateTime::from_timestamp((bi.create_ts/1000) as i64, 0);
                            let date_time: DateTime<Local> = Local.from_local_datetime(&timestamp).unwrap();
                            println!("{}\t{}", date_time, bi.name);
                        }
                    }
                }    
            } else {
                // list one bucket
                let uri = uri.unwrap();
                let re = Regex::new(SUIS3_REGEXP).unwrap();
                let caps = re.captures(&uri);
                if caps.is_none() {
                    println!("SUIS3 object format error.");
                    return;
                }
                let caps = caps.unwrap();
                let bucket_name = &caps["bucket"];
                let obj_name = &caps["object"];
    
                if obj_name.len() != 0 {
                    println!("SUIS3 object format error.");
                } else {
                    match operations::get_bucket_objects(bucket_name.to_owned()).await {
                        Err(e) => println!("{}", e),
                        Ok(ret) => {
                            println!("URI\t\t\tTIME\t\t\tSIZE\tBLOB ID\t\t\t\t\t\tTILL EPOCH");
                            for obj in ret.objects.iter() {
                                let timestamp = NaiveDateTime::from_timestamp((obj.last_write_ts/1000) as i64, 0);
                                let date_time: DateTime<Local> = Local.from_local_datetime(&timestamp).unwrap();
                                println!("{}\t{}\t{}\t{}\t{}", obj.uri, date_time, obj.size, obj.walrus_blob_id, obj.walrus_epoch_till);                                                               
                            }
                        }
                    }                    
                }
            }            
        }
        
        None | Some(SuiS3Cmd::Quit) => (), // handle in main loop
    }
}
