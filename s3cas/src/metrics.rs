use async_trait::async_trait;
use prometheus::{
    IntCounter, IntCounterVec, IntGauge, register_int_counter, register_int_counter_vec,
    register_int_gauge,
};
use s3s::S3;
use s3s::dto::*;
use s3s::{S3Request, S3Response, S3Result};
use std::{ops::Deref, sync::Arc};

const S3_API_METHODS: &[&str] = &[
    "complete_multipart_upload",
    "copy_object",
    "create_multipart_upload",
    "create_bucket",
    "delete_bucket",
    "delete_object",
    "delete_objects",
    "get_bucket_location",
    "get_object",
    "head_bucket",
    "head_object",
    "list_buckets",
    "list_objects",
    "list_objects_v2",
    "put_object",
    "upload_part",
];

#[derive(Clone, Debug)]
pub struct SharedMetrics {
    metrics: Arc<Metrics>,
}

impl SharedMetrics {
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(Metrics::new()),
        }
    }
}

impl Default for SharedMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for SharedMetrics {
    type Target = Metrics;

    fn deref(&self) -> &Self::Target {
        &self.metrics
    }
}

/// Adapter so this crate's `Metrics` can be plugged into
/// `cas_storage::SharedMetrics` (which expects `MetricsCollector`).
///
/// Note: upstream's `block_written` is parameterless so we cannot forward
/// the block size into our internal `bytes_written` counter from this path.
struct CasMetricsAdapter(Arc<Metrics>);

impl cas_storage::MetricsCollector for CasMetricsAdapter {
    fn block_pending(&self) {
        self.0.block_pending()
    }
    fn block_written(&self) {
        // size unknown via the trait; account for the block count only.
        self.0.block_written(0)
    }
    fn block_write_error(&self) {
        self.0.block_write_error()
    }
    fn block_ignored(&self) {
        self.0.block_ignored()
    }
    fn blocks_dropped(&self, amount: u64) {
        self.0.blocks_dropped(amount)
    }
    fn bytes_sent(&self, amount: usize) {
        self.0.bytes_sent(amount)
    }
    fn bytes_received(&self, amount: usize) {
        self.0.bytes_received(amount)
    }
    fn block_disk_op_started(&self) {
        self.0.block_disk_op_started()
    }
    fn block_disk_op_finished(&self) {
        self.0.block_disk_op_finished()
    }
}

impl SharedMetrics {
    /// Wrap as a cas_storage SharedMetrics (for calls into the CAS lib).
    pub fn to_cas(&self) -> cas_storage::SharedMetrics {
        cas_storage::SharedMetrics::new(Arc::new(CasMetricsAdapter(self.metrics.clone())))
    }
}
#[derive(Debug)]
pub struct Metrics {
    method_calls: IntCounterVec,
    bucket_count: IntGauge,
    data_bytes_received: IntCounter,
    data_bytes_sent: IntCounter,
    data_bytes_written: IntCounter,
    data_blocks_written: IntCounter,
    data_blocks_ignored: IntCounter,
    data_blocks_pending_write: IntGauge,
    data_blocks_write_errors: IntCounter,
    data_blocks_dropped: IntCounter,
    data_block_disk_ops_inflight: IntGauge,
}

// TODO: this can be improved, make sure this does not crash on multiple instances;
impl Metrics {
    pub fn new() -> Self {
        let method_calls = register_int_counter_vec!(
            "s3_api_method_invocations",
            "Amount of times a particular S3 API method has been called in the lifetime of the process",
            &["api_method"],
        ).expect("can register an int counter vec in the default registry");

        // instantiate the correct counters for api calls
        for api in S3_API_METHODS {
            method_calls.with_label_values(&[api]);
        }

        let bucket_count = register_int_gauge!(
            "s3_bucket_count",
            "Amount of active buckets in the S3 instance"
        )
        .expect("can register an int gauge in the default registry");

        let data_bytes_received = register_int_counter!(
            "s3_data_bytes_received",
            "Amount of bytes of actual data received"
        )
        .expect("can register an int counter in the default registry");

        let data_bytes_sent =
            register_int_counter!("s3_data_bytes_sent", "Amount of bytes of actual data sent")
                .expect("can register an int counter in the default registry");

        let data_bytes_written = register_int_counter!(
            "s3_data_bytes_written",
            "Amount of bytes of actual data written to block storage"
        )
        .expect("can register an int counter in the default registry");

        let data_blocks_written = register_int_counter!(
            "s3_data_blocks_written",
            "Amount of data blocks written to block storage"
        )
        .expect("can register an int counter in the default registry");

        let data_blocks_ignored = register_int_counter!(
            "s3_data_blocks_ignored",
            "Amount of data blocks not written to block storage, because a block with the same hash is already present"
        )
        .expect("can register an int counter in the default registry");

        let data_blocks_pending_write = register_int_gauge!(
            "s3_data_blocks_pending_write",
            "Amount of data blocks in memory, waiting to be written to block storage"
        )
        .expect("can register an int gauge in the default registry");

        let data_blocks_write_errors = register_int_counter!(
            "s3_data_blocks_write_errors",
            "Amount of data blocks which could not be written to block storage"
        )
        .expect("can register an int counter in the default registry");

        let data_blocks_dropped = register_int_counter!(
            "s3_data_blocks_dropped",
            "Amount of data blocks dropped due to client disconnects before the block was (fully) written to storage",
        ).expect("can register an int gauge in the default registry");

        let data_block_disk_ops_inflight = register_int_gauge!(
            "s3_data_block_disk_ops_inflight",
            "Blocking block-disk operations (write- and delete-side) submitted and not yet completed; sustained growth against the completion rate signals blocking-pool queue depth",
        )
        .expect("can register an int gauge in the default registry");

        Self {
            method_calls,
            bucket_count,
            data_bytes_received,
            data_bytes_sent,
            data_bytes_written,
            data_blocks_written,
            data_blocks_ignored,
            data_blocks_pending_write,
            data_blocks_write_errors,
            data_blocks_dropped,
            data_block_disk_ops_inflight,
        }
    }

    pub fn block_disk_op_started(&self) {
        self.data_block_disk_ops_inflight.inc()
    }

    pub fn block_disk_op_finished(&self) {
        self.data_block_disk_ops_inflight.dec()
    }

    pub fn add_method_call(&self, call_name: &str) {
        self.method_calls.with_label_values(&[call_name]).inc();
    }

    pub fn set_bucket_count(&self, count: usize) {
        self.bucket_count.set(count as i64)
    }

    pub fn inc_bucket_count(&self) {
        self.bucket_count.inc()
    }

    pub fn dec_bucket_count(&self) {
        self.bucket_count.dec()
    }

    pub fn bytes_received(&self, amount: usize) {
        self.data_bytes_received.inc_by(amount as u64)
    }

    pub fn bytes_sent(&self, amount: usize) {
        self.data_bytes_sent.inc_by(amount as u64)
    }

    pub fn bytes_written(&self, amount: usize) {
        self.data_bytes_written.inc_by(amount as u64)
    }

    pub fn block_pending(&self) {
        self.data_blocks_pending_write.inc()
    }

    pub fn block_written(&self, block_size: usize) {
        self.data_bytes_written.inc_by(block_size as u64);
        self.data_blocks_pending_write.dec();
        self.data_blocks_written.inc()
    }

    pub fn block_write_error(&self) {
        self.data_blocks_pending_write.dec();
        self.data_blocks_write_errors.inc()
    }

    pub fn block_ignored(&self) {
        self.data_blocks_ignored.inc()
    }

    pub fn blocks_dropped(&self, amount: u64) {
        self.data_blocks_pending_write.sub(amount as i64);
        self.data_blocks_dropped.inc_by(amount)
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

pub struct MetricFs<T> {
    storage: T,
    metrics: SharedMetrics,
}

impl<T> MetricFs<T> {
    pub fn new(storage: T, metrics: SharedMetrics) -> Self {
        Self { storage, metrics }
    }
}

#[async_trait]
impl<T> S3 for MetricFs<T>
where
    T: S3 + Sync + Send,
{
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        self.metrics.add_method_call("complete_multipart_upload");
        self.storage.complete_multipart_upload(req).await
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        self.metrics.add_method_call("copy_object");
        self.storage.copy_object(req).await
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        self.metrics.add_method_call("create_multipart_upload");
        self.storage.create_multipart_upload(req).await
    }

    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        self.metrics.add_method_call("create_bucket");
        self.storage.create_bucket(req).await
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        self.metrics.add_method_call("delete_bucket");
        self.storage.delete_bucket(req).await
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        self.metrics.add_method_call("delete_object");
        self.storage.delete_object(req).await
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        self.metrics.add_method_call("delete_objects");
        self.storage.delete_objects(req).await
    }

    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        self.metrics.add_method_call("get_bucket_location");
        self.storage.get_bucket_location(req).await
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        self.metrics.add_method_call("get_object");
        self.storage.get_object(req).await
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        self.metrics.add_method_call("head_bucket");
        self.storage.head_bucket(req).await
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        self.metrics.add_method_call("head_object");
        self.storage.head_object(req).await
    }

    async fn list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        self.metrics.add_method_call("list_buckets");
        self.storage.list_buckets(req).await
    }

    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        self.metrics.add_method_call("list_objects");
        self.storage.list_objects(req).await
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        self.metrics.add_method_call("list_objects_v2");
        self.storage.list_objects_v2(req).await
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        self.metrics.add_method_call("put_object");
        self.storage.put_object(req).await
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        self.metrics.add_method_call("upload_part");
        self.storage.upload_part(req).await
    }
}
