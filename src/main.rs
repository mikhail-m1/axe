use std::{
    env,
    fs::{create_dir_all, write, File},
    io::{ErrorKind, Read},
    iter,
    path::PathBuf,
};

use ::log::debug;
use anyhow::{Context, Result};
use aws_sdk_cloudwatchlogs as cloudwatchlogs;
use clap::{parser::ValueSource, Args, Parser, Subcommand};
use itertools::Itertools;

mod groups;
mod live_tail_client;
mod live_tail_parser;
mod log;
mod streams;
mod time_arg;
#[cfg(feature = "ui")]
mod ui;
mod utils;

#[::tokio::main]
async fn main() -> Result<()> {
    let mut args = Cli::parse();
    let mut arg_matches = <Cli as clap::CommandFactory>::command().get_matches();
    env_logger::Builder::from_env(env_logger::Env::default()).init();
    let mut config = read_config(
        &args,
        arg_matches.value_source("config_path") == Some(ValueSource::CommandLine),
    )?;

    loop {
        let Cli {
            profile,
            region,
            command,
            ..
        } = args;
        match command {
            Commands::Groups {
                verbose,
                pattern,
                streams,
            } => {
                return groups::print(
                    &create_client(&profile, &region).await.1,
                    pattern,
                    streams,
                    verbose,
                )
                .await;
            }
            Commands::Streams {
                group,
                verbose,
                prefix,
                start,
            } => {
                return streams::print(
                    &create_client(&profile, &region).await.1,
                    group,
                    prefix,
                    verbose,
                    false,
                    start,
                )
                .await;
            }
            Commands::Log(ref log_args) => {
                let (aws_config, client) = &create_client(&profile, &region).await;
                return log::print(
                    aws_config,
                    client,
                    log_args,
                    arg_matches.subcommand().unwrap().1,
                    &config,
                )
                .await;
            }
            Commands::Alias { params } => {
                if params.is_empty() {
                    return Err(anyhow::format_err!(
                        "parameters are required, use -h for help"
                    ));
                }
                if !config.contains_table("alias") {
                    config.insert("alias", toml_edit::table());
                }
                config
                    .get_key_value_mut("alias")
                    .expect("alias")
                    .1
                    .as_table_mut()
                    .expect("alias table")
                    .insert(
                        params[0].as_str(),
                        toml_edit::value(toml_edit::Array::from_iter(&params[1..])),
                    );
                debug!("added/set {params:?} to config:\n{config}");
                create_dir_all(
                    shellexpand::path::full(&args.config_path.as_path())?
                        .parent()
                        .unwrap(),
                )
                .with_context(|| {
                    format!("cannot create dirs for config file {:?}", args.config_path)
                })?;
                write(
                    shellexpand::path::full(&args.config_path.as_path())?,
                    config.to_string(),
                )
                .with_context(|| format!("writing config to {:?}", args.config_path))?;
                return Ok(());
            }
            Commands::Aliases => {
                for (key, value) in config
                    .get_key_value_mut("alias")
                    .and_then(|i| i.1.as_table())
                    .map(|t| t.get_values())
                    .into_iter()
                    .flatten()
                {
                    println!(
                        "{}\t\"{}\"",
                        key[0].get(),
                        value
                            .as_array()
                            .expect("array as value for alias")
                            .iter()
                            .map(|v| v.as_str().unwrap())
                            .join("\" \"")
                    );
                }
                return Ok(());
            }
            Commands::External(v) => {
                let app_name = env::args().next().unwrap_or("axe".into());
                let replacement = config
                    .get("alias")
                    .and_then(|v| v.as_table())
                    .and_then(|t| t.get(v[0].as_str()))
                    .and_then(|i| i.as_array())
                    .with_context(|| format!("no alias found for `{}`", v[0]))?;

                let build_iter = || {
                    iter::once(app_name.as_str())
                        .chain(
                            replacement
                                .iter()
                                .map(|v| v.as_str().expect("only are strings supported in alias")),
                        )
                        .chain(v[1..].iter().map(|s| s.as_str()))
                };
                let new_cli = Cli::try_parse_from(build_iter())
                    .with_context(|| format!("failed to parse args for alias `{}`", v[0]))?;
                debug!("alias `{}` resolved as {new_cli:?}", v[0]);

                arg_matches =
                    <Cli as clap::CommandFactory>::command().get_matches_from(build_iter());
                args = new_cli;
            }
        }
    }
}

fn read_config(args: &Cli, fail_on_not_found: bool) -> Result<toml_edit::DocumentMut> {
    debug!(
        "Try to read config from `{:?}`, fail on not found: {fail_on_not_found}",
        args.config_path
    );
    match File::open(shellexpand::path::full(&args.config_path.as_path())?) {
        Ok(mut f) => {
            let mut buf = vec![];
            f.read_to_end(&mut buf).context("cannot read config file")?;
            let config = String::from_utf8(buf)
                .context("read config at utf-8 failed")?
                .parse::<toml_edit::DocumentMut>()
                .context("config parse failed")?;
            debug!("config:\n{config}");
            Ok(config)
        }
        Err(e) if !fail_on_not_found && e.kind() == ErrorKind::NotFound => {
            Ok(toml_edit::DocumentMut::new())
        }
        Err(e) => {
            Err(e).with_context(|| format!("config not found at path {:?}", args.config_path))?
        }
    }
}

async fn create_client(
    profile: &Option<String>,
    region: &Option<String>,
) -> (aws_config::SdkConfig, cloudwatchlogs::Client) {
    let loader = aws_config::defaults(aws_config::BehaviorVersion::v2025_01_17())
        .app_name(aws_config::AppName::new("aws-axe").expect("name is valid"));

    let loader = if let Some(profile) = profile.as_ref() {
        loader.profile_name(profile)
    } else {
        loader
    };

    let loader = if let Some(region) = region.as_ref() {
        let region = aws_config::Region::new(region.clone());
        loader.region(region)
    } else {
        loader
    };

    let config = loader.load().await;
    let client = aws_sdk_cloudwatchlogs::Client::new(&config);
    (config, client)
}

#[derive(Parser, Debug)]
#[command(version, about = "AWS CloudWatch log viewer", long_about = None)]
struct Cli {
    /// AWS profile name
    #[arg(short, long)]
    profile: Option<String>,

    /// AWS region (overriding profile)
    #[arg(short, long)]
    region: Option<String>,

    /// config
    #[arg(short, long, default_value_os_t = PathBuf::from("~/.config/axe/axe.toml"))]
    config_path: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// show logs
    #[command(alias="logs")]
    Log(LogArgs),
    /// show log groups
    Groups {
        /// verbose (with size)
        #[arg(short, long)]
        verbose: bool,
        /// filter by pattern https://docs.rs/aws-sdk-cloudwatchlogs/latest/aws_sdk_cloudwatchlogs/operation/describe_log_groups/struct.DescribeLogGroupsInput.html#structfield.log_group_name_pattern
        #[arg(short, long, default_value = None)]
        pattern: Option<String>,
        /// show streams
        #[arg(short, long)]
        streams: bool,
    },
    /// show log group streams
    Streams {
        /// group name
        group: String,
        /// verbose (with details)
        #[arg(short, long)]
        verbose: bool,
        /// filter by prefix
        #[arg(short, long, default_value = None)]
        prefix: Option<String>,
        /// show events after this time, the time can be defined as
        /// * RFC 3339, ex:
        ///     * 2024-01-02T03:04:05.678Z
        ///     * 2024-01-02T03:04:05+1
        /// * offset from now, in days(d), hours(h), minutes(m), seconds(s) ex:
        ///     * 10m - 10 minutes
        ///     * 100 - 100 seconds
        ///     * 1m30s
        /// * local time of day, ex:
        ///     * 12:34
        /// * UTC time of day, ex:
        ///     * 12:34Z
        /// * Unix epoch time in seconds or milliseconds, ex:
        ///     * 1700000000
        ///     * 1700000000000
        #[arg(short, long)]
        start: Option<String>,
    },
    /// add or rewrite alias, use with with -- after alias to pass args
    Alias {
        /// Use: <alias name> -- args you want to save as the alias, ex:
        /// This is the command you want to re-run often:
        ///     cw-axe -p my-profile log my-group my-stream
        /// Create the alias by adding `alias <name> --`:
        ///     cw-axe alias my-alias --  -p my-profile log my-group my-stream
        /// Now, you can use the alias:
        ///     cw-axe my-alias
        #[arg(verbatim_doc_comment)]
        params: Vec<String>,
    },
    /// print all aliases
    Aliases,
    #[command(external_subcommand)]
    External(Vec<String>),
}

#[derive(Args, Debug)]
struct LogArgs {
    /// group name
    group: String,
    /// stream name. If not passed, will merge all streams in the Group.
    #[arg(required_unless_present = "tail")]
    stream: Option<String>,
    /// prints live log. Start, end, length and ui options are not supported.
    #[arg(short, long, default_value_t = false)]
    tail: bool,
    /// start time, the time can be defined as
    /// * RFC 3339, ex:
    ///     * 2024-01-02T03:04:05.678Z
    ///     * 2024-01-02T03:04:05+1
    /// * offset from now, in days(d), hours(h), minutes(m), seconds(s) ex:
    ///     * 10m - 10 minutes
    ///     * 100 - 100 seconds
    ///     * 1m30s
    /// * local time of day, ex:
    ///     * 12:34
    /// * UTC time of day, ex:
    ///     * 12:34Z
    /// * Unix epoch time in seconds or milliseconds, ex:
    ///     * 1700000000
    ///     * 1700000000000
    #[arg(short, long, verbatim_doc_comment, default_value_os_t = String::from("60m"))]
    start: String,
    /// end time, format is the same as for start
    #[arg(short, long, default_value = None)]
    end: Option<String>,
    /// either length or end is used, the format is same as offset for start
    #[arg(short, long, default_value = None)]
    length: Option<String>,
    /// AWS CloudWatch filter https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/FilterAndPatternSyntax.html
    /// Examples:
    /// * 'A B' - the log has to contain A and B
    /// * '"A B"' - exact match
    /// * '?A ?B' - matches with A or B
    /// * 'A -B' - matches with lines contains A and no B
    /// * '%\s[A-Z]{4}\s%' - regex
    #[arg(short, long, verbatim_doc_comment, default_value = None)]
    filter: Option<String>,
    /// replace regexp
    /// '<delimiter char><regexp><the same delimiter char><replacement>'
    /// example: '/(\d{4} [^|]+/$1'
    #[arg(short='r', long, verbatim_doc_comment, default_value = None)]
    message_regexp: Option<String>,
    /// ouput datetime format https://docs.rs/chrono/latest/chrono/format/strftime/index.html
    #[arg[short, long, default_value_t = String::from("%d%b %H:%M:%S%.3f")]]
    datetime_format: String,

    #[cfg(feature = "ui")]
    /// show results in UI
    #[arg(short, long, default_value_t = false)]
    ui: bool,

    /// number records in a chunk, maximum is 10k
    #[arg(long, default_value_t = 1000)]
    chunk_size: u16,
}
