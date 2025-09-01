use std::future::Future;

use chrono::{DateTime, Local, Utc};

pub trait OptFuture<T, F: Future<Output = T>> {
    async fn resolve(self) -> Option<T>;
}

impl<T, F: Future<Output = T>> OptFuture<T, F> for Option<F> {
    async fn resolve(self) -> Option<T> {
        if let Some(v) = self {
            Some(v.await)
        } else {
            None
        }
    }
}

pub fn local_time(timestamp: DateTime<Utc>) -> DateTime<Local> {
    timestamp.with_timezone(&Local)
}

pub fn format_opt_unix_ms(opt_unix_time_ms: Option<i64>) -> String {
    let local = Local;
    opt_unix_time_ms
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .map(|u| u.with_timezone(&local))
        .map(|d| d.to_rfc3339())
        .unwrap_or_default()
}
