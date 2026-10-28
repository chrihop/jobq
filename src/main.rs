#[cfg(not(any(target_os = "macos", target_os = "linux")))]
compile_error!("jobq requires macOS or Linux");

use anyhow::{ensure, Context as _, Result};
use base64::Engine;
use clap::{Parser, Subcommand};
use jobq::{client, config, model::*, native};
use std::{collections::BTreeMap, io::Write, path::PathBuf};

#[derive(Parser)]
#[command(version, about = "Durable local job queues for AI agents")]
struct Cli {
    #[arg(long, global = true)]
    json: bool,
    #[arg(long, global = true)]
    socket: Option<PathBuf>,
    #[command(subcommand)]
    command: Commands,
}
#[derive(Subcommand)]
enum Commands {
    Server {
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        state_dir: Option<PathBuf>,
        /// Persist maintenance before dispatch; resume explicitly after verification.
        #[arg(long)]
        maintenance: bool,
    },
    Submit {
        #[arg(long)]
        timeout: Option<String>,
        #[arg(long)]
        max_memory: Option<String>,
        #[arg(long)]
        emergency: bool,
        #[arg(long)]
        /// Block until execution and cleanup finish, then return the job's exit status.
        wait: bool,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, conflicts_with = "argv")]
        shell: Option<String>,
        #[arg(long = "env")]
        env: Vec<String>,
        #[arg(last = true)]
        argv: Vec<String>,
    },
    Status {
        id: String,
    },
    Jobs {
        #[arg(long)]
        all: bool,
    },
    Queues,
    /// Block until a job is terminal, including cleanup. Interrupting the wait leaves the job alive.
    Wait {
        #[arg(required = true, num_args = 1..)]
        ids: Vec<String>,
        /// Return when any selected job is terminal; leaves other jobs running.
        #[arg(long, conflicts_with = "all")]
        any: bool,
        /// Wait for every selected job to be terminal, including cleanup.
        #[arg(long)]
        all: bool,
        /// Bound a set wait (1..30000ms). Omitted: block until its condition is met.
        #[arg(long)]
        wait_ms: Option<u64>,
    },
    Logs {
        id: String,
        #[arg(long)]
        follow: bool,
    },
    Cancel {
        id: String,
    },
    Reload,
    /// Close admission and hold queued jobs while active work finishes.
    Drain {
        /// Finish queued jobs as well as active jobs, with admission closed.
        #[arg(long)]
        all: bool,
        /// Block until cleanup finishes and the daemon is safe to restart.
        #[arg(long)]
        wait: bool,
    },
    /// Reopen admission and dispatch after maintenance.
    Resume,
    /// Show persistent maintenance mode and restart readiness.
    Maintenance,
    /// Expose agent tools over MCP stdio; connects to the existing daemon.
    Mcp,
    #[command(name = "__supervise", hide = true)]
    Supervise {
        dir: PathBuf,
    },
    #[command(name = "__exec", hide = true)]
    Exec {
        dir: PathBuf,
    },
}
fn display(value: &serde_json::Value, json: bool) {
    if json {
        println!("{}", value);
        return;
    }
    if let (Some(id), Some(state)) = (value["id"].as_str(), value["state"].as_str()) {
        println!(
            "{id}  {state}  queue={}  memory={} bytes",
            value["queue"].as_str().unwrap_or("?"),
            value["max_memory_bytes"]
        );
        if let Some(e) = value.get("estimate").filter(|v| !v.is_null()) {
            let timestamp = |v: &serde_json::Value| {
                v.as_u64()
                    .map(|ms| {
                        humantime::format_rfc3339_millis(
                            std::time::UNIX_EPOCH + std::time::Duration::from_millis(ms),
                        )
                        .to_string()
                    })
                    .unwrap_or_else(|| "unknown".into())
            };
            println!(
                "  estimated finish: {}; budget projection: {}; confidence: {}",
                timestamp(&e["historical"]["finish_at_ms"]),
                timestamp(&e["budget"]["finish_at_ms"]),
                e["confidence"]
            );
        }
        if let Some(o) = value.get("outcome").filter(|v| !v.is_null()) {
            println!(
                "  elapsed={}ms exit={} signal={} peak sampled footprint={} bytes",
                o["elapsed_ms"],
                o["exit_code"],
                o["signal"],
                o["stats"]["peak_sampled_footprint_bytes"]
            );
            if let Some(m) = o["message"].as_str() {
                println!("  {m}");
            }
        }
    } else if let Some(rows) = value["jobs"].as_array() {
        for row in rows {
            let j = &row["job"];
            println!(
                "{}  {:<18} {}ms{}",
                j["id"].as_str().unwrap_or("?"),
                j["state"].as_str().unwrap_or("?"),
                j["queue_limit_ms"],
                if j["emergency"] == true {
                    " emergency"
                } else {
                    ""
                }
            );
        }
    } else if let Some(rows) = value["ordinary"].as_array() {
        if let Some(m) = value.get("maintenance") {
            println!("maintenance: {} ({})", m["mode"], m["phase"]);
        }
        println!(
            "runners: {}/{}; reserved memory: {} / {} bytes",
            value["active_runners"],
            value["config"]["runners"],
            value["reserved_memory_bytes"],
            value["config"]["memory_cap_bytes"]
        );
        if value["power_profile"]["enabled"] == true {
            let p = &value["power_profile"];
            println!(
                "power profile: {} (observed: {})",
                p["state"], p["observed_profile"]
            );
            if let Some(error) = p["last_error"].as_str() {
                println!("power profile warning: {error}");
            }
        }
        for q in rows {
            println!(
                "{}ms: queued={} running={} accepting={}",
                q["limit_ms"], q["queued"], q["running"], q["accepting"]
            );
        }
        println!(
            "emergency: queued={} running={}",
            value["emergency"]["queued"], value["emergency"]["running"]
        );
    } else {
        println!("{}", serde_json::to_string_pretty(value).unwrap());
    }
}
fn run(cli: Cli, inherited_umask: u32) -> Result<i32> {
    let socket = cli.socket.unwrap_or_else(native::socket_path);
    let value = match cli.command {
        Commands::Server {
            config,
            state_dir,
            maintenance,
        } => {
            jobq::server::run(
                config,
                state_dir.unwrap_or_else(native::state_dir),
                socket,
                maintenance,
            )?;
            return Ok(0);
        }
        Commands::Supervise { dir } => {
            jobq::supervisor::run(&dir)?;
            return Ok(0);
        }
        Commands::Exec { dir } => {
            jobq::supervisor::exec_job(&dir)?;
            return Ok(0);
        }
        Commands::Mcp => {
            ensure!(
                !cli.json,
                "--json cannot be used with mcp; stdout is reserved for MCP"
            );
            jobq::mcp::run(socket, inherited_umask)?;
            return Ok(0);
        }
        Commands::Submit {
            timeout,
            max_memory,
            emergency,
            wait,
            token,
            shell,
            env,
            argv,
        } => {
            let inherited = client::inherited_env()?;
            let command = if let Some(command) = shell {
                CommandSpec::Shell {
                    command,
                    shell: inherited.get("SHELL").cloned().unwrap_or("/bin/sh".into()),
                }
            } else {
                ensure!(
                    !argv.is_empty(),
                    "provide a command after -- or use --shell"
                );
                CommandSpec::Exec { argv }
            };
            let context = Context {
                command,
                cwd: std::env::current_dir()?
                    .into_os_string()
                    .into_string()
                    .map_err(|_| anyhow::anyhow!("non-UTF8 working directory"))?,
                env: inherited,
                umask: Some(inherited_umask),
            };
            let mut overrides = BTreeMap::new();
            for pair in env {
                let (k, v) = pair.split_once('=').context("--env requires NAME=value")?;
                config::validate_env(k, v)?;
                overrides.insert(k.into(), v.into());
            }
            let token = token.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let value=client::rpc(&socket,Request::Submit{token:token.clone(),context,timeout_ms:timeout.as_deref().map(config::duration).transpose()?,max_memory_bytes:max_memory.as_deref().map(config::bytes).transpose()?,emergency,env_overrides:overrides})
                .with_context(||format!("submission token {token}; if outcome is unknown, retry the same submission with --token {token}"))?;
            if wait {
                let value = client::wait(&socket, value["id"].as_str().context("missing job id")?)?;
                let code = client::outcome_exit(&value);
                display(&value, cli.json);
                return Ok(code);
            }
            value
        }
        Commands::Status { id } => client::rpc(&socket, Request::Status { id })?,
        Commands::Jobs { all } => client::rpc(
            &socket,
            Request::Jobs {
                all,
                cursor: None,
                limit: None,
            },
        )?,
        Commands::Queues => client::rpc(&socket, Request::Queues {})?,
        Commands::Cancel { id } => client::rpc(&socket, Request::Cancel { id })?,
        Commands::Reload => client::rpc(&socket, Request::Reload {})?,
        Commands::Drain { all, wait } => {
            let mut v = client::rpc(&socket, Request::Drain { all })?;
            while wait && v["ready_to_restart"] != true {
                anyhow::ensure!(
                    v["mode"] != "running",
                    "drain was resumed by another client"
                );
                std::thread::sleep(std::time::Duration::from_millis(250));
                v = client::rpc(&socket, Request::Maintenance {})?;
            }
            v
        }
        Commands::Resume => client::rpc(&socket, Request::Resume {})?,
        Commands::Maintenance => client::rpc(&socket, Request::Maintenance {})?,
        Commands::Wait {
            ids,
            any,
            all,
            wait_ms,
        } => {
            if any || all || wait_ms.is_some() {
                let v = jobq::mcp::wait_many_cli(
                    socket,
                    jobq::mcp::WaitManyParams {
                        ids,
                        mode: if any {
                            jobq::wait_set::WaitMode::Any
                        } else {
                            jobq::wait_set::WaitMode::All
                        },
                        block: wait_ms.is_none(),
                        wait_ms,
                    },
                )?;
                let code = if v["wait_expired"] == true {
                    124
                } else if v["jobs"].as_array().unwrap().iter().any(|j| {
                    jobq::wait_set::terminal(j).unwrap_or(false) && j["state"] != "succeeded"
                }) {
                    1
                } else {
                    0
                };
                if cli.json {
                    println!("{v}");
                } else {
                    println!(
                        "condition_met={} wait_expired={} completed={} pending={}",
                        v["condition_met"], v["wait_expired"], v["completed_ids"], v["pending_ids"]
                    );
                    for job in v["jobs"].as_array().unwrap() {
                        display(job, false);
                    }
                }
                return Ok(code);
            }
            ensure!(ids.len() == 1, "use --any or --all with multiple job IDs");
            let v = client::wait(&socket, &ids[0])?;
            let code = client::outcome_exit(&v);
            display(&v, cli.json);
            return Ok(code);
        }
        Commands::Logs { id, follow } => {
            let mut offsets = [0u64; 2];
            loop {
                let mut all_terminal = true;
                let mut read_any = false;
                for (i, stream) in ["stdout", "stderr"].iter().enumerate() {
                    let v = client::rpc(
                        &socket,
                        Request::Logs {
                            id: id.clone(),
                            stream: (*stream).into(),
                            offset: offsets[i],
                            max_bytes: None,
                        },
                    )?;
                    let bytes = base64::engine::general_purpose::STANDARD
                        .decode(v["data_base64"].as_str().context("missing log data")?)?;
                    offsets[i] = v["next_offset"].as_u64().context("missing log offset")?;
                    read_any |= !bytes.is_empty();
                    all_terminal &= v["terminal"] == true;
                    if cli.json {
                        println!("{v}");
                    } else if i == 0 {
                        std::io::stdout().write_all(&bytes)?;
                    } else {
                        std::io::stderr().write_all(&bytes)?;
                    }
                }
                if !read_any && (!follow || all_terminal) {
                    break;
                }
                if !read_any {
                    std::thread::sleep(std::time::Duration::from_millis(250));
                }
            }
            return Ok(0);
        }
    };
    display(&value, cli.json);
    Ok(0)
}
fn main() {
    // All daemon-created files, SQLite sidecars, and job artifacts start private.
    let inherited_umask = unsafe { libc::umask(0o077) } as u32;
    let cli = Cli::parse();
    // Even startup failures must not put ordinary JSON on the MCP transport.
    let json = cli.json && !matches!(cli.command, Commands::Mcp);
    match run(cli, inherited_umask) {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            if json {
                println!(
                    "{}",
                    serde_json::to_string(&Response::error("client_error", format!("{e:#}")))
                        .unwrap()
                );
            } else {
                eprintln!("jobq: {e:#}");
            }
            std::process::exit(2);
        }
    }
}
