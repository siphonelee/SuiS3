use anyhow::anyhow;
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};

use sui_sdk::{
    rpc_types::SuiTransactionBlockResponseOptions,
    types::{
        base_types::ObjectID,
        programmable_transaction_builder::ProgrammableTransactionBuilder,
        quorum_driver_types::ExecuteTransactionRequestType,
        transaction::{
            Argument, CallArg, Command, ProgrammableMoveCall, Transaction, TransactionData,
        },
        Identifier,
    },
};
use sui_keys::keystore::{AccountKeystore, FileBasedKeystore};

use sui_json_rpc_types::SuiObjectDataOptions;
use sui_types::transaction::ObjectArg;
use shared_crypto::intent::Intent;
use sui_config::{sui_config_dir, SUI_KEYSTORE_FILENAME};
use crate::utils;
use crate::walrus;

const PACKAGE_ID :&str = "0xaf4ce64ef7dad2b25ae3dc27165e7f7d238d046206c9a4f78dceea4cce8bd462";
const BUCKETS_ROOT :&str = "0xe3cf1909b8f9311fbfeb72ffd7f49cb30830abe5f16b7747394f970d6c2711c5";

#[serde_as]
#[derive(Deserialize, Debug)]
pub struct BucketInfo {
    pub name: String,
    #[serde_as(as = "DisplayFromStr")]
    pub create_ts: u64,
}

#[derive(Deserialize, Debug)]
pub struct BucketsList {
    pub buckets: Vec<BucketInfo>,
}

#[derive(Deserialize, Debug)]
pub struct TagsList {
    pub tags: Vec<String>,
}

#[serde_as]
#[derive(Deserialize, Debug)]
pub struct BucketObjectsInfo {
    pub uri: String,
    #[serde_as(as = "DisplayFromStr")]
    pub size: u64,
    pub tags: Vec<String>,
    #[serde_as(as = "DisplayFromStr")]
    pub last_write_ts: u64,
    pub walrus_blob_id: String,
    #[serde_as(as = "DisplayFromStr")]
    pub walrus_epoch_till: u64,
}

#[derive(Deserialize, Debug)]
pub struct BucketObjectsList {
    pub objects: Vec<BucketObjectsInfo>,
}

async fn commit_transaction(pt: sui_types::transaction::ProgrammableTransaction) -> Result<sui_json_rpc_types::SuiTransactionBlockResponse, anyhow::Error> {
    let (sui, sender, _recipient, coin) = utils::setup_for_write().await?;

    let gas_budget = 10_000_000;
    let gas_price = sui.read_api().get_reference_gas_price().await?;
    // create the transaction data that will be sent to the network
    let tx_data = TransactionData::new_programmable(
        sender.clone(),
        vec![coin.object_ref()],
        pt,
        gas_budget,
        gas_price,
    );

    // sign transaction
    let keystore = FileBasedKeystore::new(&sui_config_dir()?.join(SUI_KEYSTORE_FILENAME))?;
    let signature = keystore.sign_secure(&sender, &tx_data, Intent::sui_transaction())?;

    // execute the transaction
    let transaction_response = sui
        .quorum_driver_api()
        .execute_transaction_block(
            Transaction::from_data(tx_data, vec![signature]),
            SuiTransactionBlockResponseOptions::full_content(),
            Some(ExecuteTransactionRequestType::WaitForLocalExecution),
        )
        .await?;

    Ok(transaction_response)
}

pub async fn create_bucket(name: String) -> Result<(), anyhow::Error> {
    let mut ptb = ProgrammableTransactionBuilder::new();

    // bucketsRoot
    let sui_client = sui_sdk::SuiClientBuilder::default().build_testnet().await.unwrap();
    let object_id: ObjectID = BUCKETS_ROOT.parse().unwrap();
    let obj = sui_client.read_api().get_object_with_options(object_id, SuiObjectDataOptions::bcs_lossless()).await.unwrap().data.unwrap();
    let arg0 = CallArg::Object(ObjectArg::ImmOrOwnedObject((obj.object_id, obj.version, obj.digest)));
    ptb.input(arg0)?;

    // clock
    let arg1 = CallArg::Object(ObjectArg::SharedObject {
        id: "0x6".parse().unwrap(),
        initial_shared_version: 1.into(),
        mutable: false,
    });
    ptb.input(arg1)?;

    let arg2 = CallArg::Pure(bcs::to_bytes(&name).unwrap());
    ptb.input(arg2)?;

    let tag = "".to_owned();
    let arg3 = CallArg::Pure(bcs::to_bytes(&tag).unwrap());
    ptb.input(arg3)?;

    // add a move call to the PTB
    let package = ObjectID::from_hex_literal(PACKAGE_ID).map_err(|e| anyhow!(e))?;
    let module = Identifier::new("suis3").map_err(|e| anyhow!(e))?;
    let function = Identifier::new("create_bucket").map_err(|e| anyhow!(e))?;
    ptb.command(Command::MoveCall(Box::new(ProgrammableMoveCall {
        package,
        module,
        function,
        type_arguments: vec![],
        arguments: vec![Argument::Input(0), Argument::Input(1), Argument::Input(2), Argument::Input(3)],
    })));

    // build the transaction block by calling finish on the ptb
    let builder = ptb.finish();
    commit_transaction(builder).await?;

    Ok(())
}

pub async fn list_buckets() -> Result<Vec<BucketInfo>, anyhow::Error> {
    let mut ptb = ProgrammableTransactionBuilder::new();

    // bucketsRoot
    let sui_client = sui_sdk::SuiClientBuilder::default().build_testnet().await.unwrap();
    let object_id: ObjectID = BUCKETS_ROOT.parse().unwrap();
    let obj = sui_client.read_api().get_object_with_options(object_id, SuiObjectDataOptions::bcs_lossless()).await.unwrap().data.unwrap();
    let arg0 = CallArg::Object(ObjectArg::ImmOrOwnedObject((obj.object_id, obj.version, obj.digest)));
    ptb.input(arg0)?;

    let package = ObjectID::from_hex_literal(PACKAGE_ID).map_err(|e| anyhow!(e))?;
    let module = Identifier::new("suis3").map_err(|e| anyhow!(e))?;
    let function = Identifier::new("ls_buckets").map_err(|e| anyhow!(e))?;
    ptb.command(Command::MoveCall(Box::new(ProgrammableMoveCall {
        package,
        module,
        function,
        type_arguments: vec![],
        arguments: vec![Argument::Input(0)],
    })));

    // build the transaction block by calling finish on the ptb
    let builder = ptb.finish();

    let transaction_response = commit_transaction(builder).await?;

    let v = &transaction_response.events.unwrap().data;
    if v.len() <= 0 {
        return Err(anyhow!("Nothing returned. Your command may be incorrect."));
    }
    let ret: BucketsList = serde_json::from_str(v[0].parsed_json.to_string().as_str())?;

    Ok(ret.buckets)    
}

pub async fn delete_bucket(name: String) -> Result<(), anyhow::Error> {
    let mut ptb = ProgrammableTransactionBuilder::new();

    // bucketsRoot
    let sui_client = sui_sdk::SuiClientBuilder::default().build_testnet().await.unwrap();
    let object_id: ObjectID = BUCKETS_ROOT.parse().unwrap();
    let obj = sui_client.read_api().get_object_with_options(object_id, SuiObjectDataOptions::bcs_lossless()).await.unwrap().data.unwrap();
    let arg0 = CallArg::Object(ObjectArg::ImmOrOwnedObject((obj.object_id, obj.version, obj.digest)));
    ptb.input(arg0)?;

    let arg1 = CallArg::Pure(bcs::to_bytes(&name).unwrap());
    ptb.input(arg1)?;

    // add a move call to the PTB
    let package = ObjectID::from_hex_literal(PACKAGE_ID).map_err(|e| anyhow!(e))?;
    let module = Identifier::new("suis3").map_err(|e| anyhow!(e))?;
    let function = Identifier::new("delete_bucket").map_err(|e| anyhow!(e))?;
    ptb.command(Command::MoveCall(Box::new(ProgrammableMoveCall {
        package,
        module,
        function,
        type_arguments: vec![],
        arguments: vec![Argument::Input(0), Argument::Input(1)],
    })));

    // build the transaction block by calling finish on the ptb
    let builder = ptb.finish();
    commit_transaction(builder).await?;

    Ok(())
}

pub async fn tag_bucket(name: String, tags: Vec<String>) -> Result<(), anyhow::Error> {
    let mut ptb = ProgrammableTransactionBuilder::new();

    // bucketsRoot
    let sui_client = sui_sdk::SuiClientBuilder::default().build_testnet().await.unwrap();
    let object_id: ObjectID = BUCKETS_ROOT.parse().unwrap();
    let obj = sui_client.read_api().get_object_with_options(object_id, SuiObjectDataOptions::bcs_lossless()).await.unwrap().data.unwrap();
    let arg0 = CallArg::Object(ObjectArg::ImmOrOwnedObject((obj.object_id, obj.version, obj.digest)));
    ptb.input(arg0)?;

    let arg1 = CallArg::Pure(bcs::to_bytes(&name).unwrap());
    ptb.input(arg1)?;

    let arg2 = CallArg::Pure(bcs::to_bytes(&tags).unwrap());
    ptb.input(arg2)?;

    // add a move call to the PTB
    let package = ObjectID::from_hex_literal(PACKAGE_ID).map_err(|e| anyhow!(e))?;
    let module = Identifier::new("suis3").map_err(|e| anyhow!(e))?;
    let function = Identifier::new("tag_bucket").map_err(|e| anyhow!(e))?;
    ptb.command(Command::MoveCall(Box::new(ProgrammableMoveCall {
        package,
        module,
        function,
        type_arguments: vec![],
        arguments: vec![Argument::Input(0), Argument::Input(1), Argument::Input(2)],
    })));

    // build the transaction block by calling finish on the ptb
    let builder = ptb.finish();
    commit_transaction(builder).await?;

    Ok(())
}

pub async fn list_bucket_tags(name: String) -> Result<Vec<String>, anyhow::Error> {
    let mut ptb = ProgrammableTransactionBuilder::new();

    // bucketsRoot
    let sui_client = sui_sdk::SuiClientBuilder::default().build_testnet().await.unwrap();
    let object_id: ObjectID = BUCKETS_ROOT.parse().unwrap();
    let obj = sui_client.read_api().get_object_with_options(object_id, SuiObjectDataOptions::bcs_lossless()).await.unwrap().data.unwrap();
    let arg0 = CallArg::Object(ObjectArg::ImmOrOwnedObject((obj.object_id, obj.version, obj.digest)));
    ptb.input(arg0)?;

    let arg1 = CallArg::Pure(bcs::to_bytes(&name).unwrap());
    ptb.input(arg1)?;

    // add a move call to the PTB
    let package = ObjectID::from_hex_literal(PACKAGE_ID).map_err(|e| anyhow!(e))?;
    let module = Identifier::new("suis3").map_err(|e| anyhow!(e))?;
    let function = Identifier::new("get_bucket_tags").map_err(|e| anyhow!(e))?;
    ptb.command(Command::MoveCall(Box::new(ProgrammableMoveCall {
        package,
        module,
        function,
        type_arguments: vec![],
        arguments: vec![Argument::Input(0), Argument::Input(1)],
    })));

    // build the transaction block by calling finish on the ptb
    let builder = ptb.finish();
    let transaction_response = commit_transaction(builder).await?;

    let v = &transaction_response.events.unwrap().data;
    if v.len() <= 0 {
        return Err(anyhow!("Nothing returned. Your command may be incorrect."));
    }
    let ret: TagsList = serde_json::from_str(v[0].parsed_json.to_string().as_str())?;

    Ok(ret.tags)
}

pub async fn delete_bucket_tags(name: String) -> Result<(), anyhow::Error> {
    let mut ptb = ProgrammableTransactionBuilder::new();

    // bucketsRoot
    let sui_client = sui_sdk::SuiClientBuilder::default().build_testnet().await.unwrap();
    let object_id: ObjectID = BUCKETS_ROOT.parse().unwrap();
    let obj = sui_client.read_api().get_object_with_options(object_id, SuiObjectDataOptions::bcs_lossless()).await.unwrap().data.unwrap();
    let arg0 = CallArg::Object(ObjectArg::ImmOrOwnedObject((obj.object_id, obj.version, obj.digest)));
    ptb.input(arg0)?;

    let arg1 = CallArg::Pure(bcs::to_bytes(&name).unwrap());
    ptb.input(arg1)?;

    // add a move call to the PTB
    let package = ObjectID::from_hex_literal(PACKAGE_ID).map_err(|e| anyhow!(e))?;
    let module = Identifier::new("suis3").map_err(|e| anyhow!(e))?;
    let function = Identifier::new("delete_bucket_tags").map_err(|e| anyhow!(e))?;
    ptb.command(Command::MoveCall(Box::new(ProgrammableMoveCall {
        package,
        module,
        function,
        type_arguments: vec![],
        arguments: vec![Argument::Input(0), Argument::Input(1)],
    })));

    // build the transaction block by calling finish on the ptb
    let builder = ptb.finish();
    commit_transaction(builder).await?;

    Ok(())
}

pub async fn put_object(bucket_name: &str, obj_name: &str, filename: &String) -> Result<walrus::BlobMeta, anyhow::Error> {
    // upload to walrus
    let meta = walrus::walrus_upload_file(filename)?;
    
    // save meta data to contract
    let mut ptb = ProgrammableTransactionBuilder::new();

    // bucketsRoot
    let sui_client = sui_sdk::SuiClientBuilder::default().build_testnet().await.unwrap();
    let object_id: ObjectID = BUCKETS_ROOT.parse().unwrap();
    let obj = sui_client.read_api().get_object_with_options(object_id, SuiObjectDataOptions::bcs_lossless()).await.unwrap().data.unwrap();
    let arg0 = CallArg::Object(ObjectArg::ImmOrOwnedObject((obj.object_id, obj.version, obj.digest)));
    ptb.input(arg0)?;

    // clock
    let arg1 = CallArg::Object(ObjectArg::SharedObject {
        id: "0x6".parse().unwrap(),
        initial_shared_version: 1.into(),
        mutable: false,
    });
    ptb.input(arg1)?;

    // bucket name
    let arg2 = CallArg::Pure(bcs::to_bytes(&bucket_name).unwrap());
    ptb.input(arg2)?;

    // object name
    let arg3 = CallArg::Pure(bcs::to_bytes(&obj_name).unwrap());
    ptb.input(arg3)?;

    // size
    let arg4 = CallArg::Pure(bcs::to_bytes(&meta.size).unwrap());
    ptb.input(arg4)?;
    
    // blob id
    let arg5 = CallArg::Pure(bcs::to_bytes(&meta.walrus_blob_id).unwrap());
    ptb.input(arg5)?;

    // end epoch
    let arg6 = CallArg::Pure(bcs::to_bytes(&meta.walrus_epoch_till).unwrap());
    ptb.input(arg6)?;
    
    // tags
    let tags = Vec::<String>::new();
    let arg7 = CallArg::Pure(bcs::to_bytes(&tags).unwrap());
    ptb.input(arg7)?;

    // add a move call to the PTB
    let package = ObjectID::from_hex_literal(PACKAGE_ID).map_err(|e| anyhow!(e))?;
    let module = Identifier::new("suis3").map_err(|e| anyhow!(e))?;
    let function = Identifier::new("create_object").map_err(|e| anyhow!(e))?;
    ptb.command(Command::MoveCall(Box::new(ProgrammableMoveCall {
        package,
        module,
        function,
        type_arguments: vec![],
        arguments: vec![Argument::Input(0), Argument::Input(1),Argument::Input(2), Argument::Input(3),
                    Argument::Input(4), Argument::Input(5), Argument::Input(6), Argument::Input(7)],
    })));

    // build the transaction block by calling finish on the ptb
    let builder = ptb.finish();
    commit_transaction(builder).await?;

    Ok(meta)  
} 

pub async fn get_object_id(bucket_name: String, obj_name: String) -> Result<String, anyhow::Error> {
    let mut ptb = ProgrammableTransactionBuilder::new();

    // bucketsRoot
    let sui_client = sui_sdk::SuiClientBuilder::default().build_testnet().await.unwrap();
    let object_id: ObjectID = BUCKETS_ROOT.parse().unwrap();
    let obj = sui_client.read_api().get_object_with_options(object_id, SuiObjectDataOptions::bcs_lossless()).await.unwrap().data.unwrap();
    let arg0 = CallArg::Object(ObjectArg::ImmOrOwnedObject((obj.object_id, obj.version, obj.digest)));
    ptb.input(arg0)?;

    // bucket name
    let arg1 = CallArg::Pure(bcs::to_bytes(&bucket_name).unwrap());
    ptb.input(arg1)?;

    // object name
    let arg2 = CallArg::Pure(bcs::to_bytes(&obj_name).unwrap());
    ptb.input(arg2)?;

    // add a move call to the PTB
    let package = ObjectID::from_hex_literal(PACKAGE_ID).map_err(|e| anyhow!(e))?;
    let module = Identifier::new("suis3").map_err(|e| anyhow!(e))?;
    let function = Identifier::new("get_object").map_err(|e| anyhow!(e))?;
    ptb.command(Command::MoveCall(Box::new(ProgrammableMoveCall {
        package,
        module,
        function,
        type_arguments: vec![],
        arguments: vec![Argument::Input(0), Argument::Input(1), Argument::Input(2)],
    })));

    // build the transaction block by calling finish on the ptb
    let builder = ptb.finish();
    let transaction_response = commit_transaction(builder).await?;

    let v = &transaction_response.events.unwrap().data;
    if v.len() <= 0 {
        return Err(anyhow!("Nothing returned. Your command may be incorrect."));
    }
    // println!("{:?}", v);
    let ret: walrus::BlobMeta = serde_json::from_str(v[0].parsed_json.to_string().as_str())?;

    Ok(ret.walrus_blob_id)
}

pub async fn delete_object(bucket_name: String, obj_name: String) -> Result<(), anyhow::Error> {
    let mut ptb = ProgrammableTransactionBuilder::new();

    // bucketsRoot
    let sui_client = sui_sdk::SuiClientBuilder::default().build_testnet().await.unwrap();
    let object_id: ObjectID = BUCKETS_ROOT.parse().unwrap();
    let obj = sui_client.read_api().get_object_with_options(object_id, SuiObjectDataOptions::bcs_lossless()).await.unwrap().data.unwrap();
    let arg0 = CallArg::Object(ObjectArg::ImmOrOwnedObject((obj.object_id, obj.version, obj.digest)));
    ptb.input(arg0)?;

    // bucket name
    let arg1 = CallArg::Pure(bcs::to_bytes(&bucket_name).unwrap());
    ptb.input(arg1)?;

    // object name
    let arg2 = CallArg::Pure(bcs::to_bytes(&obj_name).unwrap());
    ptb.input(arg2)?;

    // add a move call to the PTB
    let package = ObjectID::from_hex_literal(PACKAGE_ID).map_err(|e| anyhow!(e))?;
    let module = Identifier::new("suis3").map_err(|e| anyhow!(e))?;
    let function = Identifier::new("delete_object").map_err(|e| anyhow!(e))?;
    ptb.command(Command::MoveCall(Box::new(ProgrammableMoveCall {
        package,
        module,
        function,
        type_arguments: vec![],
        arguments: vec![Argument::Input(0), Argument::Input(1), Argument::Input(2)],
    })));

    // build the transaction block by calling finish on the ptb
    let builder = ptb.finish();
    commit_transaction(builder).await?;

    Ok(())
}

pub async fn tag_object(bucket_name: String, obj_name: String, tags: Vec<String>) -> Result<(), anyhow::Error> {
    let mut ptb = ProgrammableTransactionBuilder::new();

    // bucketsRoot
    let sui_client = sui_sdk::SuiClientBuilder::default().build_testnet().await.unwrap();
    let object_id: ObjectID = BUCKETS_ROOT.parse().unwrap();
    let obj = sui_client.read_api().get_object_with_options(object_id, SuiObjectDataOptions::bcs_lossless()).await.unwrap().data.unwrap();
    let arg0 = CallArg::Object(ObjectArg::ImmOrOwnedObject((obj.object_id, obj.version, obj.digest)));
    ptb.input(arg0)?;

    let arg1 = CallArg::Pure(bcs::to_bytes(&bucket_name).unwrap());
    ptb.input(arg1)?;

    let arg2 = CallArg::Pure(bcs::to_bytes(&obj_name).unwrap());
    ptb.input(arg2)?;

    let arg3 = CallArg::Pure(bcs::to_bytes(&tags).unwrap());
    ptb.input(arg3)?;

    // add a move call to the PTB
    let package = ObjectID::from_hex_literal(PACKAGE_ID).map_err(|e| anyhow!(e))?;
    let module = Identifier::new("suis3").map_err(|e| anyhow!(e))?;
    let function = Identifier::new("tag_object").map_err(|e| anyhow!(e))?;
    ptb.command(Command::MoveCall(Box::new(ProgrammableMoveCall {
        package,
        module,
        function,
        type_arguments: vec![],
        arguments: vec![Argument::Input(0), Argument::Input(1), Argument::Input(2), Argument::Input(3)],
    })));

    // build the transaction block by calling finish on the ptb
    let builder = ptb.finish();
    commit_transaction(builder).await?;

    Ok(())
}

pub async fn list_object_tags(bucket_name: String, obj_name: String) -> Result<Vec<String>, anyhow::Error> {
    let mut ptb = ProgrammableTransactionBuilder::new();

    // bucketsRoot
    let sui_client = sui_sdk::SuiClientBuilder::default().build_testnet().await.unwrap();
    let object_id: ObjectID = BUCKETS_ROOT.parse().unwrap();
    let obj = sui_client.read_api().get_object_with_options(object_id, SuiObjectDataOptions::bcs_lossless()).await.unwrap().data.unwrap();
    let arg0 = CallArg::Object(ObjectArg::ImmOrOwnedObject((obj.object_id, obj.version, obj.digest)));
    ptb.input(arg0)?;

    // bucket name
    let arg1 = CallArg::Pure(bcs::to_bytes(&bucket_name).unwrap());
    ptb.input(arg1)?;

    // object name
    let arg2 = CallArg::Pure(bcs::to_bytes(&obj_name).unwrap());
    ptb.input(arg2)?;
    
    // add a move call to the PTB
    let package = ObjectID::from_hex_literal(PACKAGE_ID).map_err(|e| anyhow!(e))?;
    let module = Identifier::new("suis3").map_err(|e| anyhow!(e))?;
    let function = Identifier::new("get_object_tags").map_err(|e| anyhow!(e))?;
    ptb.command(Command::MoveCall(Box::new(ProgrammableMoveCall {
        package,
        module,
        function,
        type_arguments: vec![],
        arguments: vec![Argument::Input(0), Argument::Input(1), Argument::Input(2)],
    })));

    // build the transaction block by calling finish on the ptb
    let builder = ptb.finish();
    let transaction_response = commit_transaction(builder).await?;

    let v = &transaction_response.events.unwrap().data;
    if v.len() <= 0 {
        return Err(anyhow!("Nothing returned. Your command may be incorrect."));
    }
    let ret: TagsList = serde_json::from_str(v[0].parsed_json.to_string().as_str())?;

    Ok(ret.tags)
}


pub async fn delete_object_tags(bucket_name: String, obj_name: String) -> Result<(), anyhow::Error> {
    let mut ptb = ProgrammableTransactionBuilder::new();

    // bucketsRoot
    let sui_client = sui_sdk::SuiClientBuilder::default().build_testnet().await.unwrap();
    let object_id: ObjectID = BUCKETS_ROOT.parse().unwrap();
    let obj = sui_client.read_api().get_object_with_options(object_id, SuiObjectDataOptions::bcs_lossless()).await.unwrap().data.unwrap();
    let arg0 = CallArg::Object(ObjectArg::ImmOrOwnedObject((obj.object_id, obj.version, obj.digest)));
    ptb.input(arg0)?;

    let arg1 = CallArg::Pure(bcs::to_bytes(&bucket_name).unwrap());
    ptb.input(arg1)?;

    let arg2 = CallArg::Pure(bcs::to_bytes(&obj_name).unwrap());
    ptb.input(arg2)?;

    // add a move call to the PTB
    let package = ObjectID::from_hex_literal(PACKAGE_ID).map_err(|e| anyhow!(e))?;
    let module = Identifier::new("suis3").map_err(|e| anyhow!(e))?;
    let function = Identifier::new("delete_object_tags").map_err(|e| anyhow!(e))?;
    ptb.command(Command::MoveCall(Box::new(ProgrammableMoveCall {
        package,
        module,
        function,
        type_arguments: vec![],
        arguments: vec![Argument::Input(0), Argument::Input(1), Argument::Input(2)],
    })));

    // build the transaction block by calling finish on the ptb
    let builder = ptb.finish();
    commit_transaction(builder).await?;

    Ok(())
}

pub async fn get_bucket_objects(bucket_name: String) -> Result<BucketObjectsList, anyhow::Error> {
    let mut ptb = ProgrammableTransactionBuilder::new();

    // bucketsRoot
    let sui_client = sui_sdk::SuiClientBuilder::default().build_testnet().await.unwrap();
    let object_id: ObjectID = BUCKETS_ROOT.parse().unwrap();
    let obj = sui_client.read_api().get_object_with_options(object_id, SuiObjectDataOptions::bcs_lossless()).await.unwrap().data.unwrap();
    let arg0 = CallArg::Object(ObjectArg::ImmOrOwnedObject((obj.object_id, obj.version, obj.digest)));
    ptb.input(arg0)?;

    // bucket name
    let arg1 = CallArg::Pure(bcs::to_bytes(&bucket_name).unwrap());
    ptb.input(arg1)?;

    // add a move call to the PTB
    let package = ObjectID::from_hex_literal(PACKAGE_ID).map_err(|e| anyhow!(e))?;
    let module = Identifier::new("suis3").map_err(|e| anyhow!(e))?;
    let function = Identifier::new("ls_bucket_objects").map_err(|e| anyhow!(e))?;
    ptb.command(Command::MoveCall(Box::new(ProgrammableMoveCall {
        package,
        module,
        function,
        type_arguments: vec![],
        arguments: vec![Argument::Input(0), Argument::Input(1)],
    })));

    // build the transaction block by calling finish on the ptb
    let builder = ptb.finish();
    let transaction_response = commit_transaction(builder).await?;

    let v = &transaction_response.events.unwrap().data;
    if v.len() <= 0 {
        return Err(anyhow!("Nothing returned. Your command may be incorrect."));
    }
    // println!("{:?}", v[0].parsed_json.to_string().as_str());
    let ret: BucketObjectsList = serde_json::from_str(v[0].parsed_json.to_string().as_str())?;

    Ok(ret)
}