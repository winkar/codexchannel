use crate::{
    codex::{CodexClient, TurnEvent, TurnRunResult},
    commands::BotCommand,
    config::Config,
    logging,
    state::{ApprovalDecision, SessionSnapshot, SharedSessionState},
    telegram::{TelegramClient, TelegramMessage, is_telegram_poll_conflict},
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
            config.codex_sandbox_mode.clone(),
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
        let mut consecutive_poll_conflicts = 0u32;
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
                Ok(updates) => {
                    consecutive_poll_conflicts = 0;
                    updates
                }
                Err(error) => {
                    let error_text = format!("{error:#}");
                    if is_telegram_poll_conflict(&error_text) {
                        consecutive_poll_conflicts += 1;
                        logging::error(&format!(
                            "telegram polling conflict #{consecutive_poll_conflicts}: another poller is using the same bot token"
                        ));
                        if should_exit_after_poll_conflicts(consecutive_poll_conflicts) {
                            let message = concat!(
                                "telegram polling conflict persisted after 3 attempts; ",
                                "local single-instance lock is active, so another program or machine is using the same bot token via getUpdates. ",
                                "Stop the external poller or rotate TELEGRAM_BOT_TOKEN."
                            );
                            logging::error(message);
                            return Err(anyhow!(message));
                        }
                        eprintln!(
                            "telegram polling conflict: another poller is using the same bot token"
                        );
                        sleep(Duration::from_secs(self.config.poll_timeout_seconds + 2)).await;
                        continue;
                    }
                    logging::error(&format!("telegram polling failed: {error:#}"));
                    eprintln!("telegram polling failed: {error:#}");
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };
            if !updates.is_empty() {
                logging::info(&format!("received {} telegram update(s)", updates.len()));
            }
            for update in updates {
                offset = Some(update.update_id + 1);
                if let Some(message) = update.message {
                    logging::info(&format!(
                        "processing update {} message_id={} chat_id={} text={:?}",
                        update.update_id, message.message_id, message.chat.id, message.text
                    ));
                    if let Err(error) = self.handle_message(message).await {
                        logging::error(&format!(
                            "failed to process update {}: {error:#}",
                            update.update_id
                        ));
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
                logging::info("handling /start");
                self.telegram
                    .send_message(message.chat.id, &self.render_help().await)
                    .await?;
            }
            BotCommand::New => {
                logging::info("handling /new");
                if self.session.has_active_turn().await {
                    self.telegram
                        .send_message(message.chat.id, "A turn is running. Use /stop first.")
                        .await?;
                    return Ok(());
                }
                let pending = self
                    .telegram
                    .send_message(message.chat.id, "Creating new Codex thread...")
                    .await?;
                match self.codex.start_thread().await {
                    Ok(thread_id) => {
                        logging::info(&format!("created thread {thread_id}"));
                        self.session.set_active_thread(thread_id.clone()).await;
                        self.telegram
                            .edit_message(
                                message.chat.id,
                                pending.message_id,
                                &format!("Started new Codex thread:\n{thread_id}"),
                            )
                            .await?;
                    }
                    Err(error) => {
                        logging::error(&format!("failed to start thread: {error:#}"));
                        self.telegram
                            .edit_message(
                                message.chat.id,
                                pending.message_id,
                                &format!("Failed to start Codex thread:\n{error:#}"),
                            )
                            .await?;
                    }
                }
            }
            BotCommand::Use(thread_id) => {
                logging::info(&format!("handling /use {thread_id}"));
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
                logging::info("handling /status");
                let snapshot = self.session.snapshot().await;
                self.telegram
                    .send_message(message.chat.id, &render_status(&snapshot, &self.config))
                    .await?;
            }
            BotCommand::Stop => {
                logging::info("handling /stop");
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
            BotCommand::Approve { for_session } => {
                logging::info(&format!(
                    "handling /approve{}",
                    if for_session { " session" } else { "" }
                ));
                let snapshot = self.session.snapshot().await;
                if snapshot.pending_approval_message.is_none() {
                    self.telegram
                        .send_message(message.chat.id, "No pending approval.")
                        .await?;
                    return Ok(());
                }
                if for_session && !snapshot.pending_approval_supports_session {
                    self.telegram
                        .send_message(
                            message.chat.id,
                            "This approval does not support session-wide approval. Use /approve or /deny.",
                        )
                        .await?;
                    return Ok(());
                }
                let decision = if for_session {
                    ApprovalDecision::AcceptForSession
                } else {
                    ApprovalDecision::Accept
                };
                if self.session.resolve_pending_approval(decision).await {
                    self.telegram
                        .send_message(message.chat.id, approval_acknowledgement(decision))
                        .await?;
                } else {
                    self.telegram
                        .send_message(message.chat.id, "Pending approval already expired.")
                        .await?;
                }
            }
            BotCommand::Deny => {
                logging::info("handling /deny");
                if self
                    .session
                    .resolve_pending_approval(ApprovalDecision::Decline)
                    .await
                {
                    self.telegram
                        .send_message(
                            message.chat.id,
                            approval_acknowledgement(ApprovalDecision::Decline),
                        )
                        .await?;
                } else {
                    self.telegram
                        .send_message(message.chat.id, "No pending approval.")
                        .await?;
                }
            }
            BotCommand::Prompt(prompt) => {
                logging::info(&format!("handling prompt len={}", prompt.len()));
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

                let placeholder = self
                    .telegram
                    .send_message(message.chat.id, "Thinking...")
                    .await?;
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
        let last_rendered = Arc::new(Mutex::new(String::new()));
        tokio::spawn(async move {
            let result = app
                .run_turn(
                    chat_id,
                    thread_id,
                    prompt,
                    message_id,
                    cancel,
                    last_rendered.clone(),
                )
                .await;
            if let Err(error) = result {
                logging::error(&format!("turn failed: {error:#}"));
                let _ = app
                    .render_progress_message(
                        chat_id,
                        message_id,
                        &last_rendered,
                        &format!("Turn failed:\n{error:#}"),
                    )
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
        last_rendered: Arc<Mutex<String>>,
    ) -> Result<()> {
        logging::info(&format!("starting turn for thread {thread_id}"));
        let result = self
            .codex
            .run_turn(&thread_id, &prompt, cancel, |event| {
                let session = self.session.clone();
                let telegram = self.telegram.clone();
                let app = self.clone();
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
                            app.render_progress_message(chat_id, message_id, &last_rendered, &text)
                                .await?;
                        }
                        TurnEvent::Status(text) => {
                            let current = last_rendered.lock().await.clone();
                            if current.is_empty() {
                                app.render_progress_message(
                                    chat_id,
                                    message_id,
                                    &last_rendered,
                                    &text,
                                )
                                .await?;
                            }
                        }
                        TurnEvent::ApprovalRequested(approval) => {
                            let approval_message = approval.message.clone();
                            session.set_pending_approval(approval).await;
                            if last_rendered.lock().await.is_empty() {
                                app.render_progress_message(
                                    chat_id,
                                    message_id,
                                    &last_rendered,
                                    "Waiting for approval...",
                                )
                                .await?;
                            }
                            telegram.send_message(chat_id, &approval_message).await?;
                        }
                    }
                    Ok(())
                }
            })
            .await?;
        let final_cache = last_rendered.lock().await.clone();
        let final_text = finalize_text(&result, &final_cache);
        logging::info(&format!(
            "turn completed for thread {} interrupted={} final_len={}",
            thread_id,
            result.interrupted,
            final_text.len()
        ));
        if self
            .render_progress_message(chat_id, message_id, &last_rendered, &final_text)
            .await?
        {
            logging::info("rendered final telegram edit");
        } else {
            logging::info("skipping final telegram edit because content is unchanged");
        }
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
                "/stop - interrupt running turn\n",
                "/approve [session] - approve pending action\n",
                "/deny - decline pending action\n\n",
                "{}"
            ),
            render_status(&snapshot, &self.config)
        )
    }

    async fn render_progress_message(
        &self,
        chat_id: i64,
        message_id: i64,
        last_rendered: &Arc<Mutex<String>>,
        text: &str,
    ) -> Result<bool> {
        let mut current = last_rendered.lock().await;
        if text == *current {
            return Ok(false);
        }
        self.telegram
            .edit_message(chat_id, message_id, text)
            .await?;
        *current = text.to_string();
        Ok(true)
    }
}

fn should_exit_after_poll_conflicts(consecutive: u32) -> bool {
    consecutive >= 3
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
        "sandbox_mode: {}",
        config.codex_sandbox_mode.as_deref().unwrap_or("(default)")
    );
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
    if let Some(approval) = snapshot.pending_approval_message.as_deref() {
        let _ = writeln!(text, "approval_pending: yes");
        let _ = writeln!(text, "approval: {}", summarize_pending_approval(approval));
    } else {
        let _ = writeln!(text, "approval_pending: no");
    }
    text.trim_end().to_string()
}

fn finalize_text(result: &TurnRunResult, last_rendered: &str) -> String {
    if !result.assistant_text.is_empty() {
        return result.assistant_text.clone();
    }
    if !last_rendered.is_empty() {
        return last_rendered.to_string();
    }
    if result.interrupted {
        return "Turn interrupted.".to_string();
    }
    if let Some(status) = result.last_status.as_deref() {
        return status.to_string();
    }
    "Turn completed with no assistant text.".to_string()
}

fn approval_acknowledgement(decision: ApprovalDecision) -> &'static str {
    match decision {
        ApprovalDecision::Accept => "Approval accepted.",
        ApprovalDecision::AcceptForSession => "Approval accepted for this session.",
        ApprovalDecision::Decline => "Approval declined.",
        ApprovalDecision::Cancel => "Approval cancelled.",
    }
}

fn summarize_pending_approval(message: &str) -> String {
    let line = message.lines().next().unwrap_or_default().trim();
    if line.chars().count() <= 80 {
        return line.to_string();
    }
    line.chars().take(80).collect::<String>() + "..."
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
            log_path: PathBuf::from("bridge.log"),
            lock_path: PathBuf::from("bridge.lock"),
            codex_model: Some("gpt-5.4".to_string()),
            codex_approval_policy: "never".to_string(),
            codex_sandbox_mode: None,
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

    #[test]
    fn exits_after_three_poll_conflicts() {
        assert!(!should_exit_after_poll_conflicts(2));
        assert!(should_exit_after_poll_conflicts(3));
    }

    #[test]
    fn summarize_pending_approval_uses_first_line() {
        assert_eq!(
            summarize_pending_approval("Codex approval required: command execution\ncommand: dir"),
            "Codex approval required: command execution"
        );
    }
}
