use std::time::{Duration, SystemTime};

#[cfg(feature = "ui")]
use crate::ui;
use crate::utils::{local_time, OptFuture};
use crate::LogArgs;

use anyhow::{Context, Result};
use aws_sdk_cloudwatchlogs as cloudwatchlogs;
use chrono::{DateTime, Days, Local, NaiveTime};
use clap::{parser::ValueSource, ArgMatches};
use cloudwatchlogs::operation::{
    filter_log_events::builders::FilterLogEventsInputBuilder,
    get_log_events::builders::GetLogEventsInputBuilder,
};
use log::debug;
use regex::Regex;
use toml_edit::DocumentMut;

pub async fn print(
    client: &cloudwatchlogs::Client,
    args: &LogArgs,
    arg_matches: &ArgMatches,
    config: &DocumentMut,
) -> Result<()> {
    let datetime_format = if arg_matches.value_source("datetime_format")
        != Some(ValueSource::CommandLine)
        && config.contains_key("detetime_format")
    {
        config.get("datetime_format").unwrap().as_str().unwrap()
    } else {
        &args.datetime_format
    };

    let unix_now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .context("cannot get unix time as duration")?;
    let start = parse_offset_or_duration(&args.start, &unix_now)?;
    // TODO: add check for end and length at the same time
    let end = if let Some(end) = &args.end {
        parse_offset_or_duration(end, &unix_now)?
    } else if let Some(length) = &args.length {
        start
            + duration_str::parse(length)
                .with_context(|| format!("cannot parse `{length}` as duration"))?
                .as_millis() as i64
    } else {
        unix_now.as_millis() as i64
    };

    let message_regexp = args
        .message_regexp
        .as_ref()
        .map(|v| RegexWithReplace::new(v.as_str()).unwrap());

    debug!(
        "query\n from: {start} {}\n to:   {end} {}",
        local_time(start),
        local_time(end)
    );

    #[cfg(feature = "ui")]
    let mut lines = vec![];
    let mut consumer = |t: Option<i64>, m: Option<String>| {
        let m = if let Some(re) = &message_regexp {
            re.re
                .replace(&m.unwrap_or_default(), re.replacement)
                .to_string()
        } else {
            m.unwrap_or_default()
        };

        #[cfg(feature = "ui")]
        if args.ui {
            lines.push((
                format!("{}", local_time(t.unwrap_or(0)).format(datetime_format)),
                m,
            ))
        } else {
            print_event(&t, &m, datetime_format)
        }
        #[cfg(not(feature = "ui"))]
        print_event(&t, &m, datetime_format)
    };

    if let Some(filter) = &args.filter {
        print_filter_events(client, args, start, end, filter, &mut consumer).await
    } else {
        print_all_events(client, args, start, end, &mut consumer).await
    }?;

    #[cfg(feature = "ui")]
    if args.ui && !lines.is_empty() {
        ui::run(lines)
    } else {
        Ok(())
    }
    #[cfg(not(feature = "ui"))]
    Ok(())
}

async fn print_all_events<ConsumerFn>(
    client: &cloudwatchlogs::Client,
    args: &LogArgs,
    start: i64,
    end: i64,
    consumer: &mut ConsumerFn,
) -> Result<()>
where
    ConsumerFn: FnMut(Option<i64>, Option<String>),
{
    let template = GetLogEventsInputBuilder::default()
        .log_group_name(&args.group)
        .log_stream_name(&args.stream)
        .limit(args.chunk_size as i32)
        .start_from_head(true)
        .start_time(start)
        .end_time(end);

    let mut opt_res = Some(template.clone().send_with(client).await);
    while let Some(res) = opt_res {
        let output = res.context("get log events failed")?;
        if let Some(events) = output.events {
            if events.is_empty() {
                break;
            }
            for event in events.into_iter() {
                consumer(event.timestamp, event.message);
            }
        } else {
            break;
        }
        opt_res = output
            .next_forward_token
            .map(|t| template.clone().next_token(t).send_with(client))
            .resolve()
            .await;
    }
    Ok(())
}

async fn print_filter_events<ConsumerFn>(
    client: &cloudwatchlogs::Client,
    args: &LogArgs,
    start: i64,
    end: i64,
    filter: &str,
    consumer: &mut ConsumerFn,
) -> Result<()>
where
    ConsumerFn: FnMut(Option<i64>, Option<String>),
{
    let template = FilterLogEventsInputBuilder::default()
        .log_group_name(&args.group)
        .log_stream_names(&args.stream)
        .limit(args.chunk_size as i32)
        .start_time(start)
        .end_time(end)
        .filter_pattern(filter);

    let mut opt_res = Some(template.clone().send_with(client).await);
    while let Some(res) = opt_res {
        let output = res.context("filter log events failed")?;
        if let Some(events) = output.events {
            if events.is_empty() {
                break;
            }
            for event in events.into_iter() {
                consumer(event.timestamp, event.message);
            }
        } else {
            break;
        }
        opt_res = output
            .next_token
            .map(|t| template.clone().next_token(t).send_with(client))
            .resolve()
            .await;
    }
    Ok(())
}

fn print_event(timestamp: &Option<i64>, message: &str, datetime_format: &str) {
    let datetime = local_time(timestamp.unwrap_or(0)).format(datetime_format);
    println!("{datetime}|{}", message)
}

fn parse_offset_or_duration(value: &str, unix_now: &Duration) -> Result<i64> {
    duration_str::parse(value)
        .map(|o| unix_now.saturating_sub(o).as_millis() as i64)
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

struct RegexWithReplace<'a> {
    re: Regex,
    replacement: &'a str,
}

impl<'a> RegexWithReplace<'a> {
    fn new(s: &'a str) -> Result<Self> {
        let delimiter = s.chars().next().unwrap();
        let p = s
            .strip_prefix(delimiter)
            .unwrap()
            .split_once(delimiter)
            .unwrap();
        Ok(Self {
            re: Regex::new(p.0).with_context(|| format!("failed to parse {} as regex", p.0))?,
            replacement: p.1,
        })
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
        // TODO: think thow to write proper test, maybe change local time zone or just copy implementation logic
        // TODO: cover other cases
        assert!(parse_offset_or_duration("10:23", &ts).is_ok());
        assert!(parse_offset_or_duration("10:23:45", &ts).is_ok());
        assert!(parse_offset_or_duration("10:23:45.678", &ts).is_ok());
    }
}
