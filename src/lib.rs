// redis-zset-ts/src/lib.rs
//
// Copyright (c) 2024, Frank Pagliughi <fpagliughi@mindspring.com>
// All Rights Reserved
//
// Licensed under the MIT license:
//   <LICENSE or http://opensource.org/licenses/MIT>
// This file may not be copied, modified, or distributed except according
// to those terms.
//

//! Simple time-series database functionality using Redis Sorted 'Z' Sets.
//!

// Lints
#![deny(
    missing_docs,
    missing_copy_implementations,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unstable_features,
    unused_import_braces,
    unused_qualifications
)]

use rmp_serde as rmps;

mod time_series;
pub use time_series::{TimeSeries, Timestamp, TimeValue};

/// Errors for this library
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Low-level I/O error
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// MsgPack serialization error
    #[error(transparent)]
    MsgPackEncode(#[from] rmps::encode::Error),
    /// MsgPack deserialization error
    #[error(transparent)]
    MsgPackDecode(#[from] rmps::decode::Error),
    /// Redis Error
    #[error(transparent)]
    Redis(#[from] redis::RedisError),
}

/// The result type to use for the library
pub type Result<T> = std::result::Result<T, Error>;
