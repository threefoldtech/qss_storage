//! internal macros
// TODO: remove
#![allow(unused_macros)]

macro_rules! static_regex {
    ($re: literal) => {{
        use once_cell::sync::Lazy;
        use regex::Regex;

        // compile-time verified regex
        const RE: &'static str = const_str::verified_regex!($re);

        static PATTERN: Lazy<Regex> = Lazy::new(|| {
            Regex::new(RE).unwrap_or_else(|e| panic!("Invalid static regex pattern: {}", e))
        });

        &*PATTERN
    }};
}

/// extracts the error of a result in a function returning `Result<T, E>`
///
/// returns `Ok(r)` to terminate the control flow
///
macro_rules! try_err {
    ($ret:expr) => {
        match $ret {
            Ok(r) => return Ok(r),
            Err(e) => e,
        }
    };
}

/// extracts the value of a option in a function returning `Result<Option<T>, E>`
///
/// returns `Ok(None)` to terminate the control flow
///
macro_rules! try_some {
    ($opt:expr) => {
        match $opt {
            Some(r) => r,
            None => return Ok(None),
        }
    };
}

/// asserts a predicate is true in a function returning `bool`
///
/// returns `false` to terminate the control flow
///
macro_rules! bool_try {
    ($pred:expr) => {
        if !$pred {
            return false;
        }
    };
}

/// extracts the value of a option in a function returning `bool`
///
/// returns `false` to terminate the control flow
///
macro_rules! bool_try_some {
    ($opt:expr) => {
        match $opt {
            Some(r) => r,
            None => return false,
        }
    };
}

/// extracts the ok value of a result in a function returning `Result<T, E>` where E: From<S3Error>
///
/// returns an wrapped internal error to terminate the control flow
///
macro_rules! trace_try {
    ($ret:expr) => {
        match $ret {
            Ok(r) => r,
            Err(e) => return Err(internal_error!(e).into()),
        }
    };
}

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

/// Create a `NotSupported` error
macro_rules! not_supported {
    ($msg:expr) => {{ code_error!(NotSupported, $msg) }};
}

/// Create a `InvalidRequest` error
macro_rules! invalid_request {
    ($msg:expr $(, $source:expr)?) => {{
        code_error!(InvalidRequest, $msg $(, $source)?)
    }};
}

macro_rules! signature_mismatch {
    () => {{
        code_error!(
            SignatureDoesNotMatch,
            "The request signature we calculated does not match the signature you provided."
        )
    }};
}
