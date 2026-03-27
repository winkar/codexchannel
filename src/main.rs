mod app;
mod codex;
mod commands;
mod config;
mod logging;
mod singleton;
mod state;
mod telegram;

use anyhow::Result;
use app::App;
use config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::load()?;
    logging::init(&config.log_path)?;
    logging::info("starting telegram-codex-bridge");
    singleton::acquire(&config.lock_path)?;
    logging::info(&format!(
        "acquired single-instance lock at {} pid={}",
        config.lock_path.display(),
        std::process::id()
    ));
    let app = App::new(config)?;
    app.run().await
}
