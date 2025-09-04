use std::{
    future::Future,
    time::{Duration, SystemTime},
};

use chrono::{DateTime, Local};

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

pub fn local_time(unix_time_ms: i64) -> DateTime<Local> {
    DateTime::<Local>::from(
        SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_millis(unix_time_ms as u64))
            .unwrap(),
    )
}

pub fn format_opt_unix_ms(opt_unix_time_ms: Option<i64>) -> String {
    opt_unix_time_ms
        .map(local_time)
        .map(|d| d.to_rfc3339())
        .unwrap_or_default()
}
