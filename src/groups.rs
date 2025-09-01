use crate::streams;

use super::utils::OptFuture;
use anyhow::{Context, Result};
use aws_sdk_cloudwatchlogs as cloudwatchlogs;
use cloudwatchlogs::operation::describe_log_groups::builders::DescribeLogGroupsInputBuilder;
use humansize::{format_size, DECIMAL};

pub async fn print(
    client: &cloudwatchlogs::Client,
    pattern: Option<String>,
    streams: bool,
    verbose: bool,
) -> Result<()> {
    let template = DescribeLogGroupsInputBuilder::default().set_log_group_name_pattern(pattern);
    let mut groups = vec![];
    let mut opt_res = Some(template.clone().send_with(client).await);
    while let Some(res) = opt_res {
        let output = res.context("describe log groups call failed")?;
        groups.append(&mut output.log_groups.unwrap());
        opt_res = output
            .next_token
            .map(|t| template.clone().next_token(t).send_with(client))
            .resolve()
            .await;
    }
    groups.sort_by(|l, r| l.log_group_name.cmp(&r.log_group_name));
    let mut count = 0;
    let mut total_size = 0;
    for g in groups {
        if let Some(name) = g.log_group_name {
            if !verbose {
                println!("{name}");
            } else {
                println!(
                    "{name} size {}",
                    format_size(g.stored_bytes.unwrap_or(0) as u64, DECIMAL)
                )
            }
            if streams {
                streams::print(client, name, None, verbose, true, None).await?;
            }
        }
        count += 1;
        total_size += g.stored_bytes.unwrap_or_default();
    }
    println!(
        "Total: {count} groups, size: {}",
        format_size(total_size as u64, DECIMAL)
    );
    Ok(())
}
