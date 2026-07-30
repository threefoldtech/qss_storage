//! internal macros

/// extracts the ok value of a result in a function returning `S3Result<T>`
///
/// logs and returns an internal error to terminate the control flow
macro_rules! try_ {
    ($result:expr) => {
        match $result {
            Ok(val) => val,
            Err(err) => {
                error!("try_ failed {}", err);
                return Err(::s3s::S3Error::internal_error(err));
            }
        }
    };
}
