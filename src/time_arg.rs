use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Days, Local, NaiveDate, NaiveTime, TimeDelta, TimeZone, Utc};

pub fn parse(value: &str) -> Result<DateTime<Utc>> {
    parse_relative_to(value, Utc::now(), Local)
}

fn parse_as_epoch_ms(candidate: &str) -> anyhow::Result<chrono::DateTime<Utc>> {
    let ms = candidate.parse::<i64>()?;
    if ms > 946684800000 {
        // 2000-01-01 in ms
        DateTime::<Utc>::from_timestamp_millis(ms).ok_or_else(|| anyhow::anyhow!("invalid time"))
    } else {
        DateTime::<Utc>::from_timestamp(ms, 0).ok_or_else(|| anyhow::anyhow!("invalid time"))
    }
}

fn parse_as_duration_offset(value: &str, now: DateTime<Utc>) -> Result<DateTime<Utc>> {
    let duration: Duration = duration_str::parse(value).context("unable to parse duration")?;
    let time_delta =
        TimeDelta::from_std(duration).context("unable to convert duration to TimeDelta")?;
    let then = now
        .checked_sub_signed(time_delta)
        .ok_or_else(|| anyhow::anyhow!("invalid duration {}", value))?;
    Ok(then)
}

fn parse_as_bare_time<Tz>(value: &str, now: DateTime<Utc>, local_zone: Tz) -> Result<DateTime<Utc>>
where
    Tz: TimeZone,
{
    let time = NaiveTime::parse_from_str(value, "%H:%M")
        .or_else(|_| NaiveTime::parse_from_str(value, "%H:%M:%S"))
        .or_else(|_| NaiveTime::parse_from_str(value, "%H:%M:%S.%3f"))?;

    let then = now
        .with_timezone(&local_zone)
        .with_time(time)
        .single()
        .ok_or_else(|| anyhow::anyhow!("ambiguous time {}", value))?;
    let adjusted = if then > now {
        then.checked_sub_days(Days::new(1))
            .context("unable to subtract days")?
    } else {
        then
    };
    Ok(adjusted.with_timezone(&Utc))
}

fn parse_as_zoned_time(value: &str, now: DateTime<Utc>) -> Result<DateTime<Utc>> {
    let time = NaiveTime::parse_from_str(value, "%H:%MZ")
        .or_else(|_| NaiveTime::parse_from_str(value, "%H:%M:%SZ"))
        .or_else(|_| NaiveTime::parse_from_str(value, "%H:%M:%S.%3fZ"))?;

    let then = now
        .with_time(time)
        .single()
        .ok_or_else(|| anyhow::anyhow!("ambigious time"))?;
    let adjusted = if then > now {
        then.checked_sub_days(Days::new(1))
            .context("unable to subtract days")?
    } else {
        then
    };
    Ok(adjusted.with_timezone(&Utc))
}

fn parse_as_bare_date<Tz>(value: &str, local_zone: Tz) -> Result<DateTime<Utc>>
where
    Tz: TimeZone,
{
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d").context("Error parsing bare date")?;
    let midnight = NaiveTime::MIN;
    date.and_time(midnight)
        .and_local_timezone(local_zone)
        .earliest()
        .ok_or(anyhow::anyhow!("ambiguous time"))
        .map(|d| d.with_timezone(&Utc))
}

fn parse_relative_to<Tz>(value: &str, now: DateTime<Utc>, local_zone: Tz) -> Result<DateTime<Utc>>
where
    Tz: TimeZone,
{
    parse_as_epoch_ms(value)
        .or_else(|_| parse_as_duration_offset(value, now))
        .or_else(|_| parse_as_bare_time(value, now, local_zone.clone()))
        .or_else(|_| parse_as_zoned_time(value, now))
        .or_else(|_| {
            DateTime::parse_from_rfc3339(value)
                .context("error parsing RFC 3339 DateTime")
                .map(|d| d.with_timezone(&Utc))
        })
        .or_else(|_| parse_as_bare_date(value, local_zone.clone()))
        .with_context(|| {
            format!("failed to parse `{value}` as duration, time, UTC time, date or RFC3339")
        })
}

#[cfg(test)]
mod test {
    use chrono::SecondsFormat;

    use super::*;

    #[test]
    fn offset_or_duration() {
        let ts = DateTime::parse_from_rfc3339("2024-01-02T03:04:05.678Z")
            .unwrap()
            .with_timezone(&Utc);
        let local_zone = chrono_tz::US::Pacific;

        assert_eq!(
            parse_relative_to("10:23", ts, local_zone)
                .expect("should parse")
                .to_rfc3339(),
            "2024-01-01T18:23:00+00:00"
        );
        assert_eq!(
            parse_relative_to("10:23:45", ts, local_zone)
                .expect("should parse")
                .to_rfc3339(),
            "2024-01-01T18:23:45+00:00"
        );
        assert_eq!(
            parse_relative_to("10:23:45.678", ts, local_zone)
                .expect("should parse")
                .to_rfc3339_opts(SecondsFormat::Millis, true),
            "2024-01-01T18:23:45.678Z"
        );

        assert_eq!(
            parse_relative_to("1700000000", ts, local_zone)
                .unwrap()
                .timestamp_millis(),
            1700000000000
        );
        assert_eq!(
            parse_relative_to("1700000000000", ts, local_zone)
                .unwrap()
                .timestamp_millis(),
            1700000000000
        );

        assert_eq!(
            parse_relative_to("10m", ts, local_zone)
                .expect("should parse")
                .to_rfc3339(),
            "2024-01-02T02:54:05.678+00:00"
        );

        assert_eq!(
            parse_relative_to("1m30s", ts, local_zone)
                .expect("should parse")
                .to_rfc3339(),
            "2024-01-02T03:02:35.678+00:00"
        );
    }
}
