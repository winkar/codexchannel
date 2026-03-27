use crate::{
    codex::{CodexClient, TurnEvent, TurnRunResult},
    commands::BotCommand,
    config::Config,
    state::{SessionSnapshot, SharedSessionState},
    telegram::{TelegramClient, TelegramMessage},
};
use anyhow::{Result, anyhow};
use std::{fmt::Write as _, sync::Arc, time::Duration};
use tokio::{sync::Mutex, task::JoinHandle, time::sleep};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct App {
    config: Arc<Config>,
    telegram: TelegramClient,
    codex: CodexClient,
    session: SharedSessionState,
}

impl App {
    pub fn new(config: Config) -> Result<Self> {
        let telegram = TelegramClient::new(config.telegram_bot_token.clone())?;
        let codex = CodexClient::new(
            config.codex_binary.clone(),
            config.codex_cwd.clone(),
            config.codex_model.clone(),
            config.codex_approval_policy.clone(),
        );
        Ok(Self {
            config: Arc::new(config),
            telegram,
            codex,
            session: SharedSessionState::default(),
        })
    }

    pub async fn run(self) -> Result<()> {
        let mut offset: Option<i64> = None;
        loop {
            let updates = match self
                .telegram
                .get_updates(
                    offset,
                    self.config.poll_timeout_seconds,
                    self.config.update_limit,
                )
                .await
            {
                Ok(updates) => updates,
                Err(error) => {
                    eprintln!("telegram polling failed: {error:#}");
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };
            for update in updates {
                offset = Some(update.update_id + 1);
                if let Some(message) = update.message {
                    if let Err(error) = self.handle_message(message).await {
                        eprintln!("failed to process update {}: {error:#}", update.update_id);
                    }
                }
            }
        }
    }

    async fn handle_message(&self, message: TelegramMessage) -> Result<()> {
        if message.chat.kind != "private" {
            return Ok(());
        }
        let from = message
            .from
            .as_ref()
            .ok_or_else(|| anyhow!("telegram message missing from field"))?;
        if let Some(allowed_user_id) = self.config.telegram_allowed_user_id {
            if from.id != allowed_user_id {
                let _ = self
                    .telegram
                    .send_message(message.chat.id, "Unauthorized user.")
                    .await;
                return Ok(());
            }
        }

        let text = message.text.as_deref().unwrap_or("").trim();
        if text.is_empty() {
            self.telegram
                .send_message(
                    message.chat.id,
                    "Only text messages are supported in this MVP.",
                )
                .await?;
            return Ok(());
        }

        match BotCommand::parse(text) {
            BotCommand::Start => {
                self.telegram
                    .send_message(message.chat.id, &self.render_help().await)
                    .await?;
            }
            BotCommand::New => {
                if self.session.has_active_turn().await {
                    self.telegram
                        .send_message(message.chat.id, "A turn is running. Use /stop first.")
                        .await?;
                    return Ok(());
                }
                let thread_id = self.codex.start_thread().await?;
                self.session.set_active_thread(thread_id.clone()).await;
                self.telegram
                    .send_message(
                        message.chat.id,
                        &format!("Started new Codex thread:\n{thread_id}"),
                    )
                    .await?;
            }
            BotCommand::Use(thread_id) => {
                if self.session.has_active_turn().await {
                    self.telegram
                        .send_message(message.chat.id, "A turn is running. Use /stop first.")
                        .await?;
                    return Ok(());
                }
                self.session.set_active_thread(thread_id.clone()).await;
                self.telegram
                    .send_message(
                        message.chat.id,
                        &format!("Switched to Codex thread:\n{thread_id}"),
                    )
                    .await?;
            }
            BotCommand::Status => {
                let snapshot = self.session.snapshot().await;
                self.telegram
                    .send_message(message.chat.id, &render_status(&snapshot, &self.config))
                    .await?;
            }
            BotCommand::Stop => {
                if self.session.cancel_active_turn().await {
                    self.telegram
                        .send_message(message.chat.id, "Stop requested.")
                        .await?;
                } else {
                    self.telegram
                        .send_message(message.chat.id, "No active turn.")
                        .await?;
                }
            }
            BotCommand::Prompt(prompt) => {
                if self.session.has_active_turn().await {
                    self.telegram
                        .send_message(
                            message.chat.id,
                            "A turn is already running. Use /stop first.",
                        )
                        .await?;
                    return Ok(());
                }
                let Some(thread_id) = self.session.active_thread_id().await else {
                    self.telegram
                        .send_message(message.chat.id, "No active thread. Run /new first.")
                        .await?;
                    return Ok(());
                };

                let placeholder = self.telegram.send_message(message.chat.id, "⏳").await?;
                let cancel = CancellationToken::new();
                let task = self.spawn_turn_task(
                    message.chat.id,
                    thread_id,
                    prompt,
                    placeholder.message_id,
                    cancel.clone(),
                );
                self.session.set_active_turn(cancel, task).await;
            }
            BotCommand::Invalid(help) => {
                self.telegram
                    .send_message(message.chat.id, &format!("Invalid command.\n{help}"))
                    .await?;
            }
        }
        Ok(())
    }

    fn spawn_turn_task(
        &self,
        chat_id: i64,
        thread_id: String,
        prompt: String,
        message_id: i64,
        cancel: CancellationToken,
    ) -> JoinHandle<()> {
        let app = self.clone();
        tokio::spawn(async move {
            let result = app
                .run_turn(chat_id, thread_id, prompt, message_id, cancel)
                .await;
            if let Err(error) = result {
                let _ = app
                    .telegram
                    .edit_message(chat_id, message_id, &format!("Turn failed:\n{error:#}"))
                    .await;
            }
            app.session.clear_active_turn().await;
        })
    }

    async fn run_turn(
        &self,
        chat_id: i64,
        thread_id: String,
        prompt: String,
        message_id: i64,
        cancel: CancellationToken,
    ) -> Result<()> {
        let last_rendered = Arc::new(Mutex::new(String::new()));
        let result = self
            .codex
            .run_turn(&thread_id, &prompt, cancel, |event| {
                let session = self.session.clone();
                let telegram = self.telegram.clone();
                let last_rendered = last_rendered.clone();
                async move {
                    match event {
                        TurnEvent::ThreadReady(thread_id) => {
                            session.set_active_thread(thread_id).await;
                        }
                        TurnEvent::TurnStarted(turn_id) => {
                            session.set_active_turn_id(turn_id).await;
                        }
                        TurnEvent::AssistantDelta(text) => {
                            let mut current = last_rendered.lock().await;
                            if text != *current {
                                telegram.edit_message(chat_id, message_id, &text).await?;
                                *current = text;
                            }
                        }
                        TurnEvent::Status(text) => {
                            let mut current = last_rendered.lock().await;
                            if current.is_empty() {
                                telegram.edit_message(chat_id, message_id, &text).await?;
                                *current = text;
                            }
                        }
                    }
                    Ok(())
                }
            })
            .await?;
        let final_cache = last_rendered.lock().await.clone();
        let final_text = finalize_text(&result, &final_cache);
        self.telegram
            .edit_message(chat_id, message_id, &final_text)
            .await?;
        Ok(())
    }

    async fn render_help(&self) -> String {
        let snapshot = self.session.snapshot().await;
        format!(
            concat!(
                "Telegram Codex Bridge\n\n",
                "/new - start a new Codex thread\n",
                "/use <thread_id> - switch active thread\n",
                "/status - show current state\n",
                "/stop - interrupt running turn\n\n",
                "{}"
            ),
            render_status(&snapshot, &self.config)
        )
    }
}

fn render_status(snapshot: &SessionSnapshot, config: &Config) -> String {
    let mut text = String::new();
    let _ = writeln!(text, "Current status");
    let _ = writeln!(text, "cwd: {}", config.codex_cwd.display());
    let _ = writeln!(
        text,
        "model: {}",
        config.codex_model.as_deref().unwrap_or("(default)")
    );
    let _ = writeln!(text, "approval_policy: {}", config.codex_approval_policy);
    let _ = writeln!(
        text,
        "thread: {}",
        snapshot.active_thread_id.as_deref().unwrap_or("(none)")
    );
    let _ = writeln!(
        text,
        "turn_running: {}",
        if snapshot.active_turn_running {
            "yes"
        } else {
            "no"
        }
    );
    if let Some(turn_id) = snapshot.active_turn_id.as_deref() {
        let _ = writeln!(text, "turn_id: {turn_id}");
    }
    text.trim_end().to_string()
}

fn finalize_text(result: &TurnRunResult, last_rendered: &str) -> String {
    if !result.assistant_text.trim().is_empty() {
        return result.assistant_text.trim().to_string();
    }
    if !last_rendered.trim().is_empty() {
        return last_rendered.trim().to_string();
    }
    if result.interrupted {
        return "Turn interrupted.".to_string();
    }
    if let Some(status) = result.last_status.as_deref() {
        return status.to_string();
    }
    "Turn completed with no assistant text.".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use std::path::PathBuf;

    fn sample_config() -> Config {
        Config {
            telegram_bot_token: "token".to_string(),
            telegram_allowed_user_id: Some(1),
            codex_binary: PathBuf::from("codex"),
            codex_cwd: PathBuf::from("C:/work"),
            codex_model: Some("gpt-5.4".to_string()),
            codex_approval_policy: "never".to_string(),
            poll_timeout_seconds: 30,
            update_limit: 50,
        }
    }

    #[test]
    fn finalize_prefers_assistant_text() {
        let result = TurnRunResult {
            assistant_text: "hello".to_string(),
            interrupted: false,
            last_status: Some("status".to_string()),
        };
        assert_eq!(finalize_text(&result, ""), "hello");
    }

    #[test]
    fn finalize_falls_back_to_interrupt() {
        let result = TurnRunResult {
            assistant_text: String::new(),
            interrupted: true,
            last_status: Some("status".to_string()),
        };
        assert_eq!(finalize_text(&result, ""), "Turn interrupted.");
    }

    #[tokio::test]
    async fn render_status_shows_thread() {
        let session = SharedSessionState::default();
        session.set_active_thread("thread-123".to_string()).await;
        let snapshot = session.snapshot().await;
        let text = render_status(&snapshot, &sample_config());
        assert!(text.contains("thread-123"));
    }
}
