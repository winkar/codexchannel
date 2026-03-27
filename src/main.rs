mod app;
mod codex;
mod commands;
mod config;
mod state;
mod telegram;

use anyhow::Result;
use app::App;
use config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::load()?;
    let app = App::new(config)?;
    app.run().await
}
