module suis3::suis3 {
    use sui::tx_context::sender;
    use sui::vec_map::{VecMap, Self};
    use sui::package;
    use std::string::String;
    use sui::clock::Clock;
    use sui::event;

    const ENoSuchBucket: u64 = 1;
    const BucketAlreadyExists: u64 = 2;
    const ObjectAlreadyExists: u64 = 3;
    const ENoSuchObject: u64 = 4;

    public struct BlobMeta has copy, store, drop {
        size: u64,
        tags: vector<String>,
        last_write_ts: u64,
        walrus_blob_id: String,
        walrus_epoch_till: u64,
    }

    public struct BucketObject has store, drop {
        create_ts: u64,
        tags: vector<String>,
        children: VecMap<String, BlobMeta>,
    }

    public struct BucketsRoot has key, store {
        id: UID,
        current_epoch: u64,
        buckets: VecMap<String, BucketObject>,
    }

    public struct BucketInfo has copy, drop {
        name: String,
        create_ts: u64,
    }

    public struct BucketInfoEvent has copy, drop {
        buckets: vector<BucketInfo>,
    }

    public struct TagsEvent has copy, drop {
        tags: vector<String>,
    }

    public struct ObjectInfo has copy, store, drop {
        uri: String,
        size: u64,
        tags: vector<String>,
        last_write_ts: u64,
        walrus_blob_id: String,
        walrus_epoch_till: u64,
    }

    public struct BucketObjectsEvent has copy, drop {
        objects: vector<ObjectInfo>,
    }

    public struct SUIS3 has drop {}

    fun init(otw: SUIS3, ctx: &mut TxContext) {
        // Creating and sending the Publisher object to the sender.
        package::claim_and_keep(otw, ctx);

        // Creating and sending the HouseCap object to the sender.
        let bucketsRoot = BucketsRoot {
            id: object::new(ctx),
            current_epoch: 0,
            buckets: vec_map::empty(),
        };

        transfer::transfer(bucketsRoot, ctx.sender());
    }

    public fun update_epoch(bucketsRoot: &mut BucketsRoot, current_epoch: u64, _ctx: &mut TxContext) {
        bucketsRoot.current_epoch = current_epoch;
    }

    public fun create_bucket(bucketsRoot: &mut BucketsRoot, clock: &Clock, name: String, tags: vector<String>, _ctx: &mut TxContext) {
        assert!(!bucketsRoot.buckets.contains(&name), BucketAlreadyExists);
        
        let obj = BucketObject {
            create_ts: clock.timestamp_ms(),
            tags,
            children: vec_map::empty(),
        };

        bucketsRoot.buckets.insert(name, obj);
    } 

    public fun delete_bucket(bucketsRoot: &mut BucketsRoot, name: String, _ctx: &mut TxContext) {
        assert!(bucketsRoot.buckets.contains(&name), ENoSuchBucket);
        bucketsRoot.buckets.remove(&name);
    } 

    public fun ls_buckets(bucketsRoot: &mut BucketsRoot, _ctx: &mut TxContext): vector<BucketInfo> {
        let mut i = 0;
        let mut v = vector::empty<BucketInfo>();
        while (i < bucketsRoot.buckets.size()) {
            let (key, entry) = bucketsRoot.buckets.get_entry_by_idx(i);
            v.push_back(BucketInfo {
                name: *key,
                create_ts: entry.create_ts,
            });

            i = i + 1;
        };

        event::emit(BucketInfoEvent {buckets: v});
        v
    }

    public fun tag_bucket(bucketsRoot: &mut BucketsRoot, name: String, tags: vector<String>, _ctx: &mut TxContext) {
        assert!(bucketsRoot.buckets.contains(&name), ENoSuchBucket);

        let bo = bucketsRoot.buckets.get_mut(&name);
        bo.tags = tags;
    } 

    public fun get_bucket_tags(bucketsRoot: &mut BucketsRoot, name: String, _ctx: &mut TxContext): vector<String> {
        assert!(bucketsRoot.buckets.contains(&name), ENoSuchBucket);

        let v = bucketsRoot.buckets[&name].tags;
        event::emit(TagsEvent {tags: v});
        v
    }

    public fun delete_bucket_tags(bucketsRoot: &mut BucketsRoot, name: String, _ctx: &mut TxContext) {
        assert!(bucketsRoot.buckets.contains(&name), ENoSuchBucket);

        let bo = bucketsRoot.buckets.get_mut(&name);
        bo.tags = vector::empty();
    } 

    public fun create_object(bucketsRoot: &mut BucketsRoot, clock: &Clock, 
                            bucket_name: String, 
                            object_name: String, size: u64, 
                            walrus_blob_id: String, end_epoch: u64,
                            tags: vector<String>, _ctx: &mut TxContext) {
        assert!(bucketsRoot.buckets.contains(&bucket_name), ENoSuchBucket);
        let bo = bucketsRoot.buckets.get_mut(&bucket_name);
        
        let blob = BlobMeta {
            size,
            tags,
            last_write_ts: clock.timestamp_ms(),
            walrus_blob_id,
            walrus_epoch_till: end_epoch,
        };

        if (bo.children.contains(&object_name)) {
            bo.children.remove(&object_name);
        };

        bo.children.insert(object_name, blob);
    } 

    public fun get_object(bucketsRoot: &mut BucketsRoot, bucket_name: String, object_name: String): BlobMeta {
        assert!(bucketsRoot.buckets.contains(&bucket_name), ENoSuchBucket);
        let bo = bucketsRoot.buckets.get_mut(&bucket_name);
        assert!(bo.children.contains(&object_name), ENoSuchObject);
        let obj = *bo.children.get(&object_name);

        event::emit(obj);
        obj
    }

    public fun delete_object(bucketsRoot: &mut BucketsRoot, bucket_name: String, object_name: String) {
        assert!(bucketsRoot.buckets.contains(&bucket_name), ENoSuchBucket);
        let bo = bucketsRoot.buckets.get_mut(&bucket_name);
        assert!(bo.children.contains(&object_name), ENoSuchObject);
        
        bo.children.remove(&object_name);
    }

    public fun tag_object(bucketsRoot: &mut BucketsRoot, bucket_name: String, object_name: String, 
                            tags: vector<String>, _ctx: &mut TxContext) {
        assert!(bucketsRoot.buckets.contains(&bucket_name), ENoSuchBucket);
        let bo = bucketsRoot.buckets.get_mut(&bucket_name);
        assert!(bo.children.contains(&object_name), ENoSuchObject);

        let obj = bo.children.get_mut(&object_name);
        obj.tags = tags;
    } 

    public fun get_object_tags(bucketsRoot: &mut BucketsRoot, bucket_name: String, object_name: String, 
                                _ctx: &mut TxContext): vector<String> {
        assert!(bucketsRoot.buckets.contains(&bucket_name), ENoSuchBucket);
        let bo = bucketsRoot.buckets.get_mut(&bucket_name);
        assert!(bo.children.contains(&object_name), ENoSuchObject);

        let obj = bo.children.get(&object_name);
        let v = obj.tags;
        event::emit(TagsEvent {tags: v});
        v
    }

    public fun delete_object_tags(bucketsRoot: &mut BucketsRoot, bucket_name: String, object_name: String, 
                                _ctx: &mut TxContext) {
        assert!(bucketsRoot.buckets.contains(&bucket_name), ENoSuchBucket);
        let bo = bucketsRoot.buckets.get_mut(&bucket_name);
        assert!(bo.children.contains(&object_name), ENoSuchObject);

        let obj = bo.children.get_mut(&object_name);
        obj.tags = vector::empty();
    } 

    public fun ls_bucket_objects(bucketsRoot: &mut BucketsRoot, bucket_name: String, _ctx: &mut TxContext): BucketObjectsEvent {
        assert!(bucketsRoot.buckets.contains(&bucket_name), ENoSuchBucket);
        let bo = bucketsRoot.buckets.get(&bucket_name);

        let mut v = vector::empty<ObjectInfo>();

        let mut i = 0;
        while (i < bo.children.size()) {
            let (key, entry) = bo.children.get_entry_by_idx(i);
            v.push_back(ObjectInfo {
                uri: *key,
                size: entry.size,
                tags: entry.tags,
                last_write_ts: entry.last_write_ts,
                walrus_blob_id: entry.walrus_blob_id,
                walrus_epoch_till: entry.walrus_epoch_till,
            });

            i = i + 1;
        };

        let e = BucketObjectsEvent {objects: v};
        event::emit(e);
        e
    }

}

