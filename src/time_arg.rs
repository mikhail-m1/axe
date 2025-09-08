use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use chrono::{DateTime, Days, Local, NaiveTime};

pub fn unix_now() -> Result<Duration> {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .context("cannot get unix time as duration")
}

pub fn parse_offset_or_duration(value: &str, unix_now: &Duration) -> Result<i64> {
    parse_as_epoch_ms(value)
        .or_else(|_| {
            duration_str::parse(value).map(|o| unix_now.saturating_sub(o).as_millis() as i64)
        })
        .or_else(|_| {
            NaiveTime::parse_from_str(value, "%H:%M")
                .or_else(|_| NaiveTime::parse_from_str(value, "%H:%M:%S"))
                .or_else(|_| NaiveTime::parse_from_str(value, "%H:%M:%S.%3f"))
                .map_err(|_| 0)
                .and_then(|n| {
                    DateTime::from_timestamp_millis(unix_now.as_millis() as i64)
                        .unwrap()
                        .with_timezone(&Local)
                        .with_time(n)
                        .single()
                        .map(|v| {
                            if v.timestamp_millis() > (unix_now.as_millis() as i64) {
                                v.checked_sub_days(Days::new(1)).unwrap().timestamp_millis()
                            } else {
                                v.timestamp_millis()
                            }
                        })
                        .ok_or(0)
                })
        })
        .or_else(|_| {
            NaiveTime::parse_from_str(value, "%H:%MZ")
                .or_else(|_| NaiveTime::parse_from_str(value, "%H:%M:%SZ"))
                .or_else(|_| NaiveTime::parse_from_str(value, "%H:%M:%S.%3fZ"))
                .map_err(|_| 0)
                .and_then(|n| {
                    DateTime::from_timestamp_millis(unix_now.as_millis() as i64)
                        .unwrap()
                        .with_time(n)
                        .single()
                        .map(|v| {
                            if v.timestamp_millis() > (unix_now.as_millis() as i64) {
                                v.checked_sub_days(Days::new(1)).unwrap().timestamp_millis()
                            } else {
                                v.timestamp_millis()
                            }
                        })
                        .ok_or(0)
                })
        })
        .or_else(|_| DateTime::parse_from_rfc3339(value).map(|d| d.timestamp_millis()))
        .with_context(|| {
            format!("failed to parse `{value}` as duration, time, UTC time or RFC3339")
        })
}

fn parse_as_epoch_ms(candidate: &str) -> anyhow::Result<i64> {
    let ms = candidate.parse::<i64>()?;
    if ms > 946684800000 {
        // 2000-01-01 in ms
        Ok(ms)
    } else {
        Ok(ms * 1000)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn offset_or_duration() {
        let ts = Duration::from_secs(
            DateTime::parse_from_rfc3339("2024-01-02T03:04:05.678Z")
                .unwrap()
                .timestamp() as u64,
        );
        // TODO: write proper test, maybe change local time zone or just copy implementation logic
        // TODO: cover other cases
        assert!(parse_offset_or_duration("10:23", &ts).is_ok());
        assert!(parse_offset_or_duration("10:23:45", &ts).is_ok());
        assert!(parse_offset_or_duration("10:23:45.678", &ts).is_ok());

        assert_eq!(
            parse_offset_or_duration("1700000000", &ts).unwrap(),
            1700000000000
        );
        assert_eq!(
            parse_offset_or_duration("1700000000000", &ts).unwrap(),
            1700000000000
        );
    }
}
