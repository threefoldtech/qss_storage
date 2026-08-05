use s3s::S3Result;
use s3s::dto::Timestamp;
use s3s::dto::{
    Bucket, CreateBucketInput, CreateBucketOutput, DeleteBucketInput, DeleteBucketOutput,
    GetBucketLocationInput, GetBucketLocationOutput, HeadBucketInput, HeadBucketOutput,
    ListBucketsInput, ListBucketsOutput,
};
use s3s::s3_error;
use s3s::{S3Request, S3Response};
use tracing::{error, info};

use super::S3Cas;

impl S3Cas {
    pub(super) async fn create_bucket_op(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let input = req.input;

        info!("create bucket");
        if try_!(self.casfs.bucket_exists(&input.bucket)) {
            return Err(s3_error!(
                BucketAlreadyExists,
                "A bucket with this name already exists"
            ));
        }

        try_!(self.casfs.create_bucket(&input.bucket));

        self.metrics.inc_bucket_count();

        let output = CreateBucketOutput {
            location: Some(format!("/{}", input.bucket)),
        };
        Ok(S3Response::new(output))
    }

    pub(super) async fn delete_bucket_op(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let DeleteBucketInput { bucket, .. } = req.input;

        try_!(self.casfs.bucket_delete(&bucket).await);

        self.metrics.dec_bucket_count();

        Ok(S3Response::new(DeleteBucketOutput {}))
    }

    pub(super) async fn get_bucket_location_op(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        let input = req.input;
        let exists = try_!(self.casfs.bucket_exists(&input.bucket));

        if !exists {
            return Err(s3_error!(NoSuchBucket));
        }

        let output = GetBucketLocationOutput::default();
        Ok(S3Response::new(output))
    }

    pub(super) async fn head_bucket_op(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let HeadBucketInput { bucket, .. } = req.input;

        if !try_!(self.casfs.bucket_exists(&bucket)) {
            return Err(s3_error!(
                NoSuchBucket,
                "The specified bucket does not exist"
            ));
        }

        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    pub(super) async fn list_buckets_op(
        &self,
        _: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let csfs_buckets = try_!(self.casfs.list_buckets());
        let mut buckets = Vec::with_capacity(csfs_buckets.len());
        for bucket in csfs_buckets {
            let bucket = Bucket {
                creation_date: Some(Timestamp::from(bucket.ctime())),
                name: Some(bucket.name().into()),
                bucket_region: None,
            };
            buckets.push(bucket);
        }
        let output = ListBucketsOutput {
            buckets: Some(buckets),
            owner: None,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }
}
