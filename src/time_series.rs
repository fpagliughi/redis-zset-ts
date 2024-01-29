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

use crate::{as_timestamp, timestamp, Result};
use redis::{Client, Commands, Connection};
use rmp_serde as rmps;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    marker::PhantomData,
    time::{Duration, SystemTime},
};

/////////////////////////////////////////////////////////////////////////////

/// A timestamped value
pub struct TimeValue<T> {
    /// The timestamp
    pub timestamp: f64,
    /// The value
    pub value: T,
}

impl<T> TimeValue<T> {
    /// Created a new value with the current time
    pub fn new(val: T) -> Self {
        Self::with_timestamp(timestamp(), val)
    }

    /// Creates a value with the specified timestamp
    pub fn with_timestamp(ts: f64, val: T) -> Self {
        Self {
            timestamp: ts,
            value: val,
        }
    }

    /// Converts the value into a tuple
    pub fn into_tuple(self) -> (f64, T) {
        (self.timestamp, self.value)
    }
}

impl<T> From<(f64, T)> for TimeValue<T> {
    /// Create a Time value from a tuple.
    fn from(v: (f64, T)) -> Self {
        Self {
            timestamp: v.0,
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

impl<T: Copy> Copy for TimeValue<T> {}

/////////////////////////////////////////////////////////////////////////////

/// Connection to a Redis Time Series
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
    /// time stamp.
    pub fn purge(&mut self, ts: f64) -> Result<()> {
        let ts = format!("({}", ts);
        self.conn.zrembyscore(&self.key, "-inf", ts)?;
        Ok(())
    }

    /// Deletes all the values in the time series before the specified time.
    pub fn purge_time(&mut self, ts: SystemTime) -> Result<()> {
        self.purge(as_timestamp(ts))
    }

    /// Deletes all the values in the time series except points that fall in
    /// the most recent time specified by the duration.
    pub fn purge_duration(&mut self, dur: Duration) -> Result<()> {
        self.purge(timestamp() - dur.as_secs_f64())
    }

    /// Removes the entire timeseries from the Redis DB.
    pub fn delete(&mut self) -> Result<()> {
        self.conn.del(&self.key)?;
        Ok(())
    }
}

impl<T: Serialize> TimeSeries<T> {
    /// Adds a point to the time series.
    pub fn add(&mut self, ts: f64, val: T) -> Result<()> {
        let rval = rmps::encode::to_vec_named(&(ts, val))?;
        self.conn.zadd(&self.key, rval, ts)?;
        Ok(())
    }

    /// Adds a point to the time series, using the current time as the
    /// timestamp.
    pub fn add_now(&mut self, val: T) -> Result<()> {
        let ts = timestamp();
        self.add(ts, val)
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
                let rval = rmps::encode::to_vec_named(&(v.timestamp, &v.value)).unwrap();
                (v.timestamp, rval)
            })
            .collect();

        self.conn.zadd_multiple(&self.key, &rvals)?;
        Ok(())
    }

    /// Adds multiple points to the time series from a slice of tuples.
    pub fn add_multiple_values(&mut self, vals: &[(f64, T)]) -> Result<()> {
        let rvals: Vec<(f64, Vec<u8>)> = vals
            .iter()
            .map(|(ts, v)| (*ts, rmps::encode::to_vec_named(&(ts, v)).unwrap()))
            .collect();

        self.conn.zadd_multiple(&self.key, &rvals)?;
        Ok(())
    }
}

impl<T: DeserializeOwned> TimeSeries<T> {
    /// Gets values from a time range
    /// The timestamps can be the usual floating point values, or anything
    /// that can be conbverted to a Redis argument, such as special strings
    /// like "-inf", "+inf", or "2.0)" to indicate open ranges.
    pub fn get_range_any<R, RR>(&mut self, ts1: R, ts2: RR) -> Result<Vec<TimeValue<T>>>
    where
        R: redis::ToRedisArgs,
        RR: redis::ToRedisArgs,
    {
        let v: Vec<Vec<u8>> = self.conn.zrangebyscore(&self.key, ts1, ts2)?;
        let vret = v
            .iter()
            .map(|buf| rmps::decode::from_slice(buf).unwrap())
            .map(|(timestamp, value)| TimeValue { timestamp, value })
            .collect();
        Ok(vret)
    }

    /// Gets values from a time range
    /// This gets the values from `ts1` up to, but not including, `ts2`.
    pub fn get_range(&mut self, ts1: f64, ts2: f64) -> Result<Vec<TimeValue<T>>> {
        let ts2 = format!("({}", ts2);
        self.get_range_any::<_, _>(ts1, ts2)
    }

    /// Gets values from a time range
    /// This gets the values from `ts1` up to, but not including, `ts2`.
    pub fn get_time_range(
        &mut self,
        ts1: SystemTime,
        ts2: SystemTime,
    ) -> Result<Vec<TimeValue<T>>> {
        self.get_range(as_timestamp(ts1), as_timestamp(ts2))
    }

    /// Gets values starting from the specified time up to the latest value.
    pub fn get_range_from(&mut self, ts: f64) -> Result<Vec<TimeValue<T>>> {
        self.get_range_any::<_, _>(ts, "+inf")
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

    const NAMESPACE: &str = "redis-ts";

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

        let v = series.get_range_from(3.0).unwrap();
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

        series.purge(3.0).unwrap();

        let v = series.get_range(1.0, 5.0).unwrap();
        assert_eq!(2, v.len());
        assert_eq!(99, v[0].value);
        assert_eq!(13, v[1].value);
    }
}
