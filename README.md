# A library for using Redis as a simple Time-Series Database

This library allows you to use a plain vanilla installation of Redis to hold simple time-series data. This is not meant to replace a time-series database; if you have one available, use it. Similarly, Redis has one or more plugins that allow you to store time-series data in a more efficient manner. If that is available, it is likely prefereble to using this method of storing data.

But if you are already using Redis, and are restricted from adding and maintaining special plugins, this library can be useful for caching and maintaining time-series data.  

Redis is primarily a key/value store, but has a number of additional features for storing different data types. One, called "sorted sets" was originally created to do website vote ranking, but can be used inversely to track time series data using a floating-point *(time_t)* values as the timestamp.

In doing this the "key" of the sorted set becomes the data being tracked, and the "score" is the time index. The main caveat of doing this is that the key of the sorted set (and thus our data) must be unique. To insure uniqueness, the data can be combined with the timestamp in a tuple or array before being sent to Redis. This creates a redundant timestamp for each point, but actually simplifies data retrieval, because the index does not need to be recombined to get the time/data pair. The values retrieved from Redis have all the time/data information.

## Database Connection

Using this library, each data set is represented by the Rust `TimeSeries<T>` generic type. This contains a connection to the database that can be used to add or query for data for a single time series.

The series is specified in Redis as a sorted "Z" set, identified with a namespace and series name. The namespace might typically be the name of the application creating the data. That would then allow the application to use any names for the timeseries without worry about a name clash. Only the namespace would need to be unique on the host.

It can be used in Rust like:

    use redis-zset-ts::TimeSeries;

    let mut series = TimeSeries::<f64>("analog", "chan0").unwrap();

This would create a connection to Redis over localhost to manage a time series for namespace _analog_, with the series name, _chan0_. Each data point would be a single 64-bit floating point value.

Within the database, the namespace and series name are simply concatenated with a colon, ":". Therefore this series name in Redis would be "analog:chan0".

Normally in an Edge Device or IoT Gateway we would use the local Redis database on the same host, but the library can also be used to connect to an arbitrary host, or any URI, respectively:

```Rust
pub fn with_host(host: &str, namespace: &str, name: &str) -> Result<Self> { ... }
pub fn with_uri(uri: &str, namespace: &str, name: &str) -> Result<Self> { ... }
```

## The Time Series

Within the application that uses this library, data points can be represented a `TimeValue<T>`:

```Rust
/// A timestamped value
pub struct TimeValue<T> {
    /// The timestamp
    pub timestamp: f64,
    /// The value
    pub value: T,
}
```

To be fully used with Redis, the type T must be serializable and deserializable into an owned value with serde, thus:

```Rust
T: serde::Serialize + serde::DeserializeOwned
```

As a convenience, the time value can also be converted to and from, and manipulated as a tuple of the form:

```Rust
(f64, T)
```
Like:

```Rust
let tv = TimeValue::from((0.0, 42));
let tup = tv.into_tuple();
```

The _timestamp_ is a 64-bit floating point representation of the number of seconds since the UNIX epoch (i.e. floating-point *time_t*), with at least microseconds resolution. It is directly compatible with the Python time function, *time.time()*. It can be obtained for the current time or from any Rust `SystemTime`:

```Rust
let ts = redis_zset_ts::timestamp();    // The current time

use std::time::{SystemTime, Duration};
let sys_time = SystemTime::now() - Duration::from_secs(10);
let ts = redis_zset_ts::as_timestamp(sys_time);   // 10 seconds ago

let ts = redis__zset_ts::timestamp() - 10.0;    // also 10 seconds ago
```

The data is put into Redis using MsgPack to combine the time and value into a tuple, then serialized into a byte stream, like this:

```Rust
rmps::encode::to_vec_named(&(v.timestamp, &v.value))
```

## Data Insertion

Values can be inserted into the Redis database using individual points like:

```Rust
let v: i32 = read_some_value();
series.add(timestamp(), v);
```

Or just using the current time as:

```Rust
series.add_now(v);
```

Or using a _TimeValue:_

```Rust
let tv = TimeValue::new(v);    // Uses the current time
series.add_value(tv);
```

Multiple values can be added efficiently all at once:

```Rust
let vals: Vec<_> = (0..5)
    .into_iter()
    .map(|_| TimeValue::new(read_some_values())
    .collect();

series.add_multiple(&vals);
```

## Data Retrieval

Data can be queried using a time range. The range can be provided as floating-point timestamps or with `SystemTime` values. Using timestamps makes it convenient to do ranges of seconds with simple subtraction:

```Rust
// Retrieve the last minute of data
let now = timestamp();
let vals = series.get_range(now-60.0, now).unwrap();
```

The time points can use strings like "-inf" and "+inf" which are special to Redis:

```Rust
// Gets the whole time series (all points)
let vals = series.get_range_any("-inf", "+inf").unwrap();
```

## Removing Data

The oldest data can be easily purged, using a time point to erase any values prior to it:

```Rust
// Erase any data older than 10min
let ts = timestamp();
series.purge(ts - 600.0);
```

This can be done periodically or any time adding new points to keep a moving window of data in Redis, like the last minute, ten minutes, hour, day, etc.

```Rust
let ts = timestamp();
series.add(ts, read_some_value()); // Insert a new value, then
series.purge(ts - 600.0);          // erase any data older than 10min
```

The _entire_ series can be removed from Redis with a single call:

```Rust
series.delete();
```

# Language and System Compatibility

Redis clients exist for all major languages. This library serializes the data into Redis using MsgPack, which also has implementations in a large number of languages. It should be trivial to share data created by this library with most languages and platforms.
