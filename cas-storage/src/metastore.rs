mod block;
mod bucket_meta;
pub(crate) mod codec;
mod content_hash;
mod errors;
mod meta_store;
mod object;
/// The one module of the metastore that stays public: s3cas's inspect tool
/// classifies a directory before opening it, with `classify_db_dir`,
/// `STORE_HEADER_MAGIC` and `STORE_HEADER_VERSION` -- items the re-exports
/// below do not carry.
pub mod store_header;
mod stores;
mod traits;
mod upload_record;

pub use block::{BLOCKID_SIZE, Block, BlockId, MAX_BLOCKID_SIZE, block_disk_path};
pub use bucket_meta::BucketMeta;
pub use content_hash::{CONTENT_HASH_SIZE, ContentHash};
pub use errors::{FsError, MetaError, StorePairingMismatch};
pub use meta_store::{
    BlockDecrement, BlockTree, DEFAULT_BLOCK_TREE, MULTIPART_PARTS_TREE, MetaStore, Transaction,
    UPLOADS_TREE,
};
// The backend a Transaction wraps: crate-internal, so the only implementor is
// the one in stores/.
pub(crate) use meta_store::TransactionBackend;
pub use object::{Object, ObjectData, ObjectType};
pub use store_header::{HeaderSpec, StoreHeader, StoreHeaderError, StoreId, StoreInit};
pub use stores::FjallStore;
pub use traits::{BaseMetaTree, Durability, KeyValuePairs, MetaTreeExt, Store};
pub use upload_record::UploadRecord;
