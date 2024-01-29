// redis-ts/src/lib.rs
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
use std::{
    io,
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

mod time_series;
pub use time_series::TimeSeries;

/// Converts the system time to a 64-bit floating point value which
/// represents the number of seconds, including fraction, since the Unix
/// Epoch. This has microsecond resolution.
pub fn as_timestamp(st: SystemTime) -> f64 {
    let ts = st
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs_f64();
    (ts * 1.0e6).round() / 1.0e6
}

/// Gets the current time as a 64-bit floating point value which represents
/// the number of seconds, including fraction, since the Unix Epoch.
/// This has microsecond resolution.
pub fn timestamp() -> f64 {
    as_timestamp(SystemTime::now())
}

/// Errors for this library
#[derive(Error, Debug)]
pub enum Error {
    /// Low-level I/O error
    #[error(transparent)]
    Io(#[from] io::Error),
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
