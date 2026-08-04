#![forbid(unsafe_code)]
#![deny(
    clippy::all, //
    clippy::must_use_candidate, //
)]

#[macro_use]
#[path = "it_s3/common.rs"]
mod common;

#[path = "it_s3/objects.rs"]
mod objects;

#[path = "it_s3/listing.rs"]
mod listing;

#[path = "it_s3/multipart.rs"]
mod multipart;

#[path = "it_s3/multipart_listing.rs"]
mod multipart_listing;
