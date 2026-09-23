use std::process::ExitCode;

use modbus_senml_gateway::config;
use modbus_senml_gateway::syslog::SyslogLayer;
use tokio::signal::unix::{signal, SignalKind};
use tracing::error;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

const DEFAULT_CONFIG_PATH: &str = "/etc/modbus-gateway/config.toml";

#[tokio::main]
async fn main() -> ExitCode {
    let path = std::env::args_os()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_CONFIG_PATH.into());

    // Logging isn't up yet (its tag and level come from the config), so a
    // config error goes straight to stderr.
    let config = match config::load(&path) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("modbus-senml-gateway: {e}");
            return ExitCode::FAILURE;
        }
    };

    let level: LevelFilter = config
        .gateway
        .log_level
        .parse()
        .expect("log_level is checked by config validation");
    tracing_subscriber::registry()
        .with(SyslogLayer::new(config.gateway.id.clone()).with_filter(level))
        .init();

    // Registered before `run` starts anything, so an early SIGTERM isn't
    // lost to the default disposition.
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, "failed to install SIGTERM handler");
            return ExitCode::FAILURE;
        }
    };
    let shutdown = async move {
        tokio::select! {
            _ = sigterm.recv() => {}
            // Convenience for running in a foreground terminal.
            _ = tokio::signal::ctrl_c() => {}
        }
    };

    match modbus_senml_gateway::run(config, shutdown).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!(error = %e, "gateway failed");
            ExitCode::FAILURE
        }
    }
}
