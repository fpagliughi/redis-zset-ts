// redis-zset-ts/src/time_series.rs
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

use crate::Result;
use redis::{Client, Commands, Connection};
use rmp_serde as rmps;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fmt,
    marker::PhantomData,
    ops::{Add, AddAssign, Sub, SubAssign},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/////////////////////////////////////////////////////////////////////////////

/// The timestamp for values in the database.
///
/// This is an absolute time point, represented as a floating point time_t
/// value with arbitrary resolutions, typically in the microsecond range.
#[derive(Default, Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Timestamp(f64);

impl Timestamp {
    /// Creates a timestamp for the current time.
    pub fn now() -> Self {
        Self::from(SystemTime::now())
    }

    /// Creates a timestamp with a specific resolution
    pub fn with_resolution(st: SystemTime, res: Duration) -> Self {
        Self::with_resolution_f64(st, res.as_secs_f64())
    }

    /// Creates a timestamp with a resolution specified as floating point
    /// seconds.
    ///
    /// So, for microsecond resolution use 1.0e-6; for millisecond
    /// resolution, 1.0e-3, etc.
    pub fn with_resolution_f64(st: SystemTime, res: f64) -> Self {
        let ts = st
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs_f64();
        Self(res * (ts / res).round())
    }

    /// Gets the time stamp as a floating-point time_t value.
    #[inline]
    pub fn as_f64(&self) -> f64 {
        self.0
    }

    /// Converts the time stamp to a system time value.
    pub fn into_system_time(self) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs_f64(self.0)
    }
}

impl From<f64> for Timestamp {
    fn from(val: f64) -> Self {
        Self(val)
    }
}

impl From<SystemTime> for Timestamp {
    fn from(st: SystemTime) -> Self {
        let ts = st
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs_f64();
        Self(1.0e-6 * (ts / 1.0e-6).round())
    }
}

impl From<Timestamp> for SystemTime {
    fn from(ts: Timestamp) -> Self {
        UNIX_EPOCH + Duration::from_secs_f64(ts.0)
    }
}

impl Add<f64> for Timestamp {
    type Output = Self;

    fn add(self, dur: f64) -> Self::Output {
        Self(self.0 + dur)
    }
}

impl AddAssign<f64> for Timestamp {
    fn add_assign(&mut self, dur: f64) {
        self.0 += dur
    }
}

impl Sub<f64> for Timestamp {
    type Output = Self;

    fn sub(self, dur: f64) -> Self::Output {
        Self(self.0 - dur)
    }
}

impl SubAssign<f64> for Timestamp {
    fn sub_assign(&mut self, dur: f64) {
        self.0 -= dur
    }
}

impl Add<Duration> for Timestamp {
    type Output = Self;

    fn add(self, dur: Duration) -> Self::Output {
        Self(self.0 + dur.as_secs_f64())
    }
}

impl AddAssign<Duration> for Timestamp {
    fn add_assign(&mut self, dur: Duration) {
        self.0 += dur.as_secs_f64()
    }
}

impl Sub<Duration> for Timestamp {
    type Output = Self;

    fn sub(self, dur: Duration) -> Self::Output {
        Self(self.0 - dur.as_secs_f64())
    }
}

impl SubAssign<Duration> for Timestamp {
    fn sub_assign(&mut self, dur: Duration) {
        self.0 -= dur.as_secs_f64()
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/////////////////////////////////////////////////////////////////////////////

/// A timestamped value.
///
/// This is a data value along with the time at which it was
/// created/collected.
pub struct TimeValue<T> {
    /// The timestamp at which the value was collected.
    pub timestamp: Timestamp,
    /// The value
    pub value: T,
}

impl<T> TimeValue<T> {
    /// Created a new value with the current time
    pub fn new(val: T) -> Self {
        Self::with_timestamp(Timestamp::now(), val)
    }

    /// Creates a value with the specified timestamp
    pub fn with_timestamp<S>(ts: S, val: T) -> Self
    where
        S: Into<Timestamp>,
    {
        Self {
            timestamp: ts.into(),
            value: val,
        }
    }

    /// Converts the value into a tuple
    pub fn into_tuple(self) -> (Timestamp, T) {
        (self.timestamp, self.value)
    }

    /// Converts the value into a tuple
    pub fn into_tuple_f64(self) -> (f64, T) {
        (self.timestamp.0, self.value)
    }
}

impl<S: Into<Timestamp>, T> From<(S, T)> for TimeValue<T> {
    fn from(v: (S, T)) -> Self {
        Self {
            timestamp: v.0.into(),
            value: v.1,
        }
    }
}

impl<T: Clone> Clone for TimeValue<T> {
    fn clone(&self) -> Self {
        Self {
            timestamp: self.timestamp,
            value: self.value.clone(),
        }
    }
}

/// Time values that contain copyable data are also copyable.
impl<T: Copy> Copy for TimeValue<T> {}

/////////////////////////////////////////////////////////////////////////////

/// Connection to a Redis Time Series
///
/// This contains a connection to the Redis database that can be used to
/// create, delete, and interact with a single time series.
///
/// To be completely readable and writeable, the data type, `T` must
/// implement the traits `Clone`, `serde::Serialize` and
/// `serde::DeserializeOwned`.
pub struct TimeSeries<T> {
    /// The name/key of the Redis sorted set.
    key: String,
    /// The Redis client
    #[allow(dead_code)]
    cli: Client,
    /// The Redis connection
    conn: Connection,
    /// Placeholder for data type
    phantom: PhantomData<T>,
}

impl<T> TimeSeries<T> {
    /// Creates a new connection to the named time series.
    pub fn new(namespace: &str, name: &str) -> Result<Self> {
        Self::with_host("localhost", namespace, name)
    }

    /// Creates a new connection to the named time series on the specified
    /// host.
    pub fn with_host(host: &str, namespace: &str, name: &str) -> Result<Self> {
        Self::with_uri(&format!("redis://{}/", host), namespace, name)
    }

    /// Creates a new connection to the named time series on the host with
    /// the specified URI.
    pub fn with_uri(uri: &str, namespace: &str, name: &str) -> Result<Self> {
        let key = if namespace.is_empty() {
            name.into()
        }
        else {
            format!("{}:{}", namespace, name)
        };
        let cli = Client::open(uri)?;
        let conn = cli.get_connection()?;
        Ok(Self {
            key,
            cli,
            conn,
            phantom: PhantomData,
        })
    }

    /// Deletes all the values in the time series before the specified
    /// time stamp, `ts`.
    pub fn purge_before<S>(&mut self, ts: S) -> Result<()>
    where
        S: Into<Timestamp>,
    {
        let ts = format!("({}", ts.into());
        self.conn.zrembyscore(&self.key, "-inf", ts)?;
        Ok(())
    }

    /// Deletes all the values in the time series older than the specified
    /// duration.
    ///
    /// This deletes all the data except points that fall in the most recent
    /// time specified by the duration.
    pub fn purge_older_than(&mut self, dur: Duration) -> Result<()> {
        self.purge_before(SystemTime::now() - dur)
    }

    /// Removes the entire timeseries from the Redis DB.
    pub fn delete(&mut self) -> Result<()> {
        self.conn.del(&self.key)?;
        Ok(())
    }
}

impl<T: Serialize> TimeSeries<T> {
    /// Adds a point to the time series.
    pub fn add<S>(&mut self, ts: S, val: T) -> Result<()>
    where
        S: Into<Timestamp>,
    {
        let ts = ts.into();
        let rval = rmps::encode::to_vec_named(&(ts.0, val))?;
        self.conn.zadd(&self.key, rval, ts.0)?;
        Ok(())
    }

    /// Adds a point to the time series, using the current time as the
    /// timestamp.
    #[inline]
    pub fn add_now(&mut self, val: T) -> Result<()> {
        self.add(Timestamp::now(), val)
    }

    /// Adds a point to the time series as a TimeValue.
    pub fn add_value<V>(&mut self, val: V) -> Result<()>
    where
        V: Into<TimeValue<T>>,
    {
        let val = val.into();
        self.add(val.timestamp, val.value)
    }

    /// Adds multiple points to the time series.
    pub fn add_multiple(&mut self, vals: &[TimeValue<T>]) -> Result<()> {
        let rvals: Vec<(f64, Vec<u8>)> = vals
            .iter()
            .map(|v| {
                let rval = rmps::encode::to_vec_named(&(v.timestamp.0, &v.value)).unwrap();
                (v.timestamp.0, rval)
            })
            .collect();

        self.conn.zadd_multiple(&self.key, &rvals)?;
        Ok(())
    }

    /// Adds multiple points to the time series from a slice of tuples.
    pub fn add_multiple_values(&mut self, vals: &[(Timestamp, T)]) -> Result<()> {
        let rvals: Vec<(f64, Vec<u8>)> = vals
            .iter()
            .map(|(ts, v)| (ts.0, rmps::encode::to_vec_named(&(ts.0, v)).unwrap()))
            .collect();

        self.conn.zadd_multiple(&self.key, &rvals)?;
        Ok(())
    }
}

impl<T: DeserializeOwned> TimeSeries<T> {
    /// Gets values from an arbitrary time range.
    /// The timestamps can be floating point values, or anything that can be
    /// converted to a Redis argument, such as special strings like "-inf",
    /// "+inf", or "2.0)" to indicate open ranges.
    pub fn get_range_any<S, U>(&mut self, ts1: S, ts2: U) -> Result<Vec<TimeValue<T>>>
    where
        S: redis::ToRedisArgs,
        U: redis::ToRedisArgs,
    {
        let v: Vec<Vec<u8>> = self.conn.zrangebyscore(&self.key, ts1, ts2)?;
        let vret = v
            .iter()
            .map(|buf| rmps::decode::from_slice(buf).unwrap())
            .map(|(ts, value): (f64, T)| TimeValue {
                timestamp: Timestamp::from(ts),
                value,
            })
            .collect();
        Ok(vret)
    }

    /// Gets values from a time range.
    /// This gets the values from `ts1` up to, but not including, `ts2`.
    pub fn get_range<S, U>(&mut self, ts1: S, ts2: U) -> Result<Vec<TimeValue<T>>>
    where
        S: Into<Timestamp>,
        U: Into<Timestamp>,
    {
        let ts2 = format!("({}", ts2.into());
        self.get_range_any::<_, _>(ts1.into().0, ts2)
    }

    /// Gets values starting from the specified time up to the latest value.
    pub fn get_from<S>(&mut self, ts: S) -> Result<Vec<TimeValue<T>>>
    where
        S: Into<Timestamp>,
    {
        self.get_range_any::<_, _>(ts.into().0, "+inf")
    }

    /// Gets the most recent points for the specified duration.
    pub fn get_last(&mut self, dur: Duration) -> Result<Vec<TimeValue<T>>> {
        self.get_from(SystemTime::now() - dur)
    }

    /// Gets all the values in the series.
    /// This should be used with caution if the series is large.
    pub fn get_all(&mut self) -> Result<Vec<TimeValue<T>>> {
        self.get_range_any::<_, _>("-inf", "+inf")
    }
}

/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    const NAMESPACE: &str = "redis-zset-ts";

    #[test]
    fn test_time_value() {
        let tv = TimeValue::from((0.0, 42));
        assert_eq!(42, tv.value);

        let tv = tv.into_tuple();
        assert_eq!(42, tv.1);
    }

    #[test]
    fn test_conn() {
        let mut series = TimeSeries::<i32>::new(NAMESPACE, "conn").unwrap();
        let _ = series.delete();
    }

    #[test]
    fn test_add() {
        let mut series = TimeSeries::new(NAMESPACE, "add").unwrap();
        let _ = series.delete();
        series.add_now(&42).unwrap();
    }

    #[test]
    fn test_get() {
        let mut series = TimeSeries::new(NAMESPACE, "get").unwrap();
        let _ = series.delete();

        series.add(2.0, 42).unwrap();
        series.add(3.0, 99).unwrap();
        series.add_value((4.0, 13)).unwrap();

        let v = series.get_range(2.0, 3.0).unwrap();
        assert_eq!(1, v.len());
        assert_eq!(42, v[0].value);

        let v = series.get_range(2.0, 4.0).unwrap();
        assert_eq!(2, v.len());
        assert_eq!(42, v[0].value);
        assert_eq!(99, v[1].value);

        let v = series.get_from(3.0).unwrap();
        assert_eq!(2, v.len());
        assert_eq!(99, v[0].value);
        assert_eq!(13, v[1].value);

        let v = series.get_all().unwrap();
        assert_eq!(3, v.len());
        assert_eq!(42, v[0].value);
        assert_eq!(99, v[1].value);
        assert_eq!(13, v[2].value);
    }

    #[test]
    fn test_purge() {
        let mut series = TimeSeries::new(NAMESPACE, "purge").unwrap();
        let _ = series.delete();

        series.add(2.0, 42).unwrap();
        series.add(3.0, 99).unwrap();
        series.add_value((4.0, 13)).unwrap();

        series.purge_before(3.0).unwrap();

        let v = series.get_range(1.0, 5.0).unwrap();
        assert_eq!(2, v.len());
        assert_eq!(99, v[0].value);
        assert_eq!(13, v[1].value);
    }
}
