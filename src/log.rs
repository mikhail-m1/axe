use std::io::Write;

use crate::time_arg::{parse_offset_or_duration, unix_now};
#[cfg(feature = "ui")]
use crate::ui;
use crate::utils::{local_time, OptFuture};
use crate::{live_tail_client, LogArgs};

use anyhow::{Context, Result};
use aws_credential_types::provider::ProvideCredentials;
use aws_sdk_cloudwatchlogs as cloudwatchlogs;
use aws_sdk_cloudwatchlogs::operation::describe_log_groups::builders::DescribeLogGroupsInputBuilder;
use clap::{parser::ValueSource, ArgMatches};
use cloudwatchlogs::operation::{
    filter_log_events::builders::FilterLogEventsInputBuilder,
    get_log_events::builders::GetLogEventsInputBuilder,
};
use log::debug;
use regex::Regex;
use toml_edit::DocumentMut;

pub async fn print(
    aws_config: &aws_config::SdkConfig,
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

    let message_regexp = args
        .message_regexp
        .as_ref()
        .map(|v| RegexWithReplace::new(v.as_str()).unwrap());

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
            ));
            true
        } else {
            print_event(&t, &m, datetime_format)
        }
        #[cfg(not(feature = "ui"))]
        print_event(&t, &m, datetime_format)
    };

    if args.tail {
        if args.ui {
            anyhow::bail!("UI doesn't work with tail");
        }
        if args.end.is_some() || args.length.is_some() {
            anyhow::bail!("tail doesn't support end nor length parameters")
        }
        return tail(aws_config, client, args, &mut consumer).await;
    }

    let now = unix_now()?;
    let start = parse_offset_or_duration(&args.start, &now)?;
    // TODO: add check for end and length at the same time
    let end = if let Some(end) = &args.end {
        parse_offset_or_duration(end, &now)?
    } else if let Some(length) = &args.length {
        start
            + duration_str::parse(length)
                .with_context(|| format!("cannot parse `{length}` as duration"))?
                .as_millis() as i64
    } else {
        now.as_millis() as i64
    };

    debug!(
        "query\n from: {start} {}\n to:   {end} {}",
        local_time(start),
        local_time(end)
    );

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

async fn tail(
    aws_config: &aws_config::SdkConfig,
    client: &aws_sdk_cloudwatchlogs::Client,
    args: &LogArgs,
    consumer: &mut impl FnMut(Option<i64>, Option<String>) -> bool,
) -> Result<()> {
    let descriptions = DescribeLogGroupsInputBuilder::default()
        .set_log_group_name_prefix(Some(args.group.clone()))
        .limit(1)
        .send_with(client)
        .await?;
    let arn = descriptions
        .log_groups
        .as_ref()
        .and_then(|gs| gs.first())
        .and_then(|g| g.arn())
        .ok_or_else(|| {
            anyhow::anyhow!("Failed to get arn, DescribeLogGroup response: {descriptions:?}")
        })?;
    live_tail_client::request_and_process(
        &aws_config
            .credentials_provider()
            .unwrap()
            .provide_credentials()
            .await?,
        aws_config.region().expect("region is provided").as_ref(),
        arn.trim_end_matches("*"),
        args.stream.as_deref(),
        args.filter.as_deref(),
        consumer,
    )
    .await?;
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
    ConsumerFn: FnMut(Option<i64>, Option<String>) -> bool,
{
    let template = GetLogEventsInputBuilder::default()
        .log_group_name(&args.group)
        // clap ensures that this option is present unless --tail is passed
        .log_stream_name(args.stream.as_ref().unwrap())
        .limit(args.chunk_size as i32)
        .start_from_head(true)
        .start_time(start)
        .end_time(end);

    let mut opt_res = Some(template.clone().send_with(client).await);
    'main: while let Some(res) = opt_res {
        let output = res.context("get log events failed")?;
        if let Some(events) = output.events {
            if events.is_empty() {
                break;
            }
            for event in events.into_iter() {
                if !consumer(event.timestamp, event.message) {
                    break 'main;
                }
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
    ConsumerFn: FnMut(Option<i64>, Option<String>) -> bool,
{
    let template = FilterLogEventsInputBuilder::default()
        .log_group_name(&args.group)
        // clap ensures that this option is present unless --tail is passed
        .log_stream_names(args.stream.as_deref().unwrap())
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
                if !consumer(event.timestamp, event.message) {
                    break;
                }
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

fn print_event(timestamp: &Option<i64>, message: &str, datetime_format: &str) -> bool {
    let datetime = local_time(timestamp.unwrap_or(0)).format(datetime_format);
    let mut lock = std::io::stdout().lock();
    let result = writeln!(lock, "{datetime}|{}", message);
    match result {
        Ok(()) => true,
        Err(e) => {
            eprint!("Cannot write to stdout: {e}");
            false
        }
    }
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
