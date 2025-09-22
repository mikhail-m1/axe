use crate::{
    time_arg::{parse_offset_or_duration, unix_now},
    utils::format_opt_unix_ms,
};

use super::utils::OptFuture;
use anyhow::{Context, Result};
use aws_sdk_cloudwatchlogs::{self as cloudwatchlogs, types::OrderBy};
use cloudwatchlogs::{
    operation::describe_log_streams::builders::DescribeLogStreamsInputBuilder, types::LogStream,
};

pub async fn print(
    client: &cloudwatchlogs::Client,
    group: String,
    prefix: Option<String>,
    verbose: bool,
    tab: bool,
    start: Option<String>,
) -> Result<()> {
    let mut streams: Vec<LogStream> = vec![];

    let mut template = DescribeLogStreamsInputBuilder::default()
        .set_log_group_name(Some(group))
        .set_log_stream_name_prefix(prefix);

    if start.is_some() {
        template = template.order_by(OrderBy::LastEventTime).descending(true)
    } else {
        template = template.order_by(OrderBy::LogStreamName).descending(false);
    }

    let start_timestamp = if let Some(start) = &start {
        parse_offset_or_duration(start, &unix_now()?)?
    } else {
        0
    };

    let mut opt_res = Some(template.clone().send_with(client).await);
    'outer: while let Some(res) = opt_res {
        let mut output = res.context("describe log streams call failed")?;
        if let Some(ref mut output_streams) = output.log_streams {
            for stream in output_streams.drain(..) {
                if let Some(timestamp) = stream.last_event_timestamp() {
                    if timestamp < start_timestamp {
                        break 'outer;
                    }
                }
                streams.push(stream);
            }
        }
        opt_res = output
            .next_token
            .map(|t| template.clone().next_token(t).send_with(client))
            .resolve()
            .await;
    }
    streams.sort_by(|l, r| l.log_stream_name.cmp(&r.log_stream_name));
    for s in streams {
        if let Some(name) = s.log_stream_name {
            if !verbose {
                println!("{name}");
            } else {
                println!(
                    "{}{name} first {:?} last {:?}",
                    if tab { "\t" } else { "" },
                    format_opt_unix_ms(s.first_event_timestamp),
                    format_opt_unix_ms(s.last_event_timestamp),
                )
            }
        }
    }
    Ok(())
}
