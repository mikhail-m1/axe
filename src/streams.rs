use crate::utils::format_opt_unix_ms;

use super::utils::OptFuture;
use anyhow::{Context, Result};
use aws_sdk_cloudwatchlogs as cloudwatchlogs;
use cloudwatchlogs::operation::describe_log_streams::builders::DescribeLogStreamsInputBuilder;

pub async fn print(
    client: &cloudwatchlogs::Client,
    group: String,
    prefix: Option<String>,
    verbose: bool,
    tab: bool,
) -> Result<()> {
    let mut streams = vec![];

    let template = DescribeLogStreamsInputBuilder::default()
        .set_log_group_name(Some(group))
        .set_log_stream_name_prefix(prefix);

    let mut opt_res = Some(template.clone().send_with(client).await);
    while let Some(res) = opt_res {
        let output = res.context("describe log streams call failed")?;
        streams.append(&mut output.log_streams.unwrap());
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
