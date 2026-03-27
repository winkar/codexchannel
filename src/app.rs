use crate::{
    codex::{CodexClient, TurnEvent, TurnRunResult},
    commands::BotCommand,
    config::Config,
    logging,
    state::{ApprovalDecision, SessionSnapshot, SharedSessionState},
    telegram::{TelegramClient, TelegramMessage, is_telegram_poll_conflict},
};
use anyhow::{Result, anyhow};
use std::{
    fmt::Write as _,
    fs,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{Mutex, Notify},
    task::JoinHandle,
    time::{Instant, sleep, sleep_until},
};
use tokio_util::sync::CancellationToken;

const TELEGRAM_EDIT_THROTTLE: Duration = Duration::from_millis(400);

#[derive(Clone)]
pub struct App {
    config: Arc<Config>,
    telegram: TelegramClient,
    codex: CodexClient,
    session: SharedSessionState,
}

impl App {
    pub fn new(config: Config) -> Result<Self> {
        let initial_cwd = config.codex_cwd.clone();
        let imported_history = load_local_codex_cwd_history().unwrap_or_else(|error| {
            logging::error(&format!(
                "failed loading local codex cwd history: {error:#}"
            ));
            Vec::new()
        });
        let telegram = TelegramClient::new(config.telegram_bot_token.clone())?;
        let codex = CodexClient::new(
            config.codex_binary.clone(),
            initial_cwd.clone(),
            config.codex_model.clone(),
            config.codex_approval_policy.clone(),
            config.codex_sandbox_mode.clone(),
        );
        Ok(Self {
            config: Arc::new(config),
            telegram,
            codex,
            session: SharedSessionState::new_with_history(initial_cwd, imported_history),
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
            BotCommand::Cwd(argument) => {
                if self.session.has_active_turn().await {
                    self.telegram
                        .send_message(message.chat.id, "A turn is running. Use /stop first.")
                        .await?;
                    return Ok(());
                }

                match argument {
                    None => {
                        let snapshot = self.session.snapshot().await;
                        self.telegram
                            .send_message(message.chat.id, &render_cwd_history(&snapshot))
                            .await?;
                    }
                    Some(argument) => {
                        let cwd = self.resolve_requested_cwd(&argument).await?;
                        self.codex.set_cwd(cwd.clone()).await;
                        self.session.set_active_cwd(cwd.clone()).await;
                        self.telegram
                            .send_message(
                                message.chat.id,
                                &format!(
                                    "Switched working directory to:\n{}\n\n{}",
                                    cwd.display(),
                                    render_cwd_history(&self.session.snapshot().await)
                                ),
                            )
                            .await?;
                    }
                }
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
        let progress = ProgressRenderer::new(self.telegram.clone(), chat_id, message_id);
        let render_task = tokio::spawn(progress.clone().run());
        tokio::spawn(async move {
            let result = app
                .run_turn(chat_id, thread_id, prompt, cancel, progress.clone())
                .await;
            if let Err(error) = result {
                logging::error(&format!("turn failed: {error:#}"));
                progress.finish(&format!("Turn failed:\n{error:#}")).await;
            }
            match render_task.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    logging::error(&format!("progress render failed: {error:#}"));
                }
                Err(error) => {
                    logging::error(&format!("progress render task join failed: {error}"));
                }
            }
            app.session.clear_active_turn().await;
        })
    }

    async fn run_turn(
        &self,
        chat_id: i64,
        thread_id: String,
        prompt: String,
        cancel: CancellationToken,
        progress: ProgressRenderer,
    ) -> Result<()> {
        logging::info(&format!("starting turn for thread {thread_id}"));
        let result = self
            .codex
            .run_turn(&thread_id, &prompt, cancel, |event| {
                let session = self.session.clone();
                let telegram = self.telegram.clone();
                let progress = progress.clone();
                async move {
                    match event {
                        TurnEvent::ThreadReady(thread_id) => {
                            session.set_active_thread(thread_id).await;
                        }
                        TurnEvent::TurnStarted(turn_id) => {
                            session.set_active_turn_id(turn_id).await;
                        }
                        TurnEvent::AssistantDelta(text) => {
                            progress.update(&text).await;
                        }
                        TurnEvent::Status(text) => {
                            let current = progress.current_text().await;
                            if current.is_empty() {
                                progress.update(&text).await;
                            }
                        }
                        TurnEvent::ApprovalRequested(approval) => {
                            let approval_message = approval.message.clone();
                            session.set_pending_approval(approval).await;
                            if progress.current_text().await.is_empty() {
                                progress.update("Waiting for approval...").await;
                            }
                            telegram.send_message(chat_id, &approval_message).await?;
                        }
                    }
                    Ok(())
                }
            })
            .await?;
        let final_cache = progress.current_text().await;
        let final_text = finalize_text(&result, &final_cache);
        logging::info(&format!(
            "turn completed for thread {} interrupted={} final_len={}",
            thread_id,
            result.interrupted,
            final_text.len()
        ));
        progress.finish(&final_text).await;
        Ok(())
    }

    async fn render_help(&self) -> String {
        let snapshot = self.session.snapshot().await;
        format!(
            concat!(
                "Telegram Codex Bridge\n\n",
                "/new - start a new Codex thread\n",
                "/use <thread_id> - switch active thread\n",
                "/cwd [path|index] - show or switch working directory\n",
                "/status - show current state\n",
                "/stop - interrupt running turn\n",
                "/approve [session] - approve pending action\n",
                "/deny - decline pending action\n\n",
                "{}"
            ),
            render_status(&snapshot, &self.config)
        )
    }
}

#[derive(Clone)]
struct ProgressRenderer {
    inner: Arc<ProgressRendererInner>,
}

struct ProgressRendererInner {
    telegram: TelegramClient,
    chat_id: i64,
    message_id: i64,
    notify: Notify,
    state: Mutex<ProgressState>,
}

#[derive(Default)]
struct ProgressState {
    desired: String,
    rendered: String,
    closed: bool,
}

impl ProgressRenderer {
    fn new(telegram: TelegramClient, chat_id: i64, message_id: i64) -> Self {
        Self {
            inner: Arc::new(ProgressRendererInner {
                telegram,
                chat_id,
                message_id,
                notify: Notify::new(),
                state: Mutex::new(ProgressState::default()),
            }),
        }
    }

    async fn update(&self, text: &str) {
        let mut state = self.inner.state.lock().await;
        if state.closed || state.desired == text {
            return;
        }
        state.desired = text.to_string();
        drop(state);
        self.inner.notify.notify_one();
    }

    async fn finish(&self, text: &str) {
        let mut state = self.inner.state.lock().await;
        state.desired = text.to_string();
        state.closed = true;
        drop(state);
        self.inner.notify.notify_one();
    }

    async fn current_text(&self) -> String {
        let state = self.inner.state.lock().await;
        if state.desired.is_empty() {
            state.rendered.clone()
        } else {
            state.desired.clone()
        }
    }

    async fn run(self) -> Result<()> {
        let mut next_allowed = Instant::now();
        loop {
            let snapshot = {
                let state = self.inner.state.lock().await;
                (state.desired.clone(), state.rendered.clone(), state.closed)
            };
            let (desired, rendered, closed) = snapshot;

            if desired == rendered {
                if closed {
                    logging::info("skipping final telegram edit because content is unchanged");
                    return Ok(());
                }
                self.inner.notify.notified().await;
                continue;
            }

            let now = Instant::now();
            let can_render_now = rendered.is_empty() || closed || now >= next_allowed;
            if !can_render_now {
                tokio::select! {
                    _ = sleep_until(next_allowed) => {}
                    _ = self.inner.notify.notified() => {}
                }
                continue;
            }

            self.inner
                .telegram
                .edit_message(self.inner.chat_id, self.inner.message_id, &desired)
                .await?;
            let mut state = self.inner.state.lock().await;
            state.rendered = desired.clone();
            let done = state.closed && state.rendered == state.desired;
            drop(state);
            next_allowed = Instant::now() + TELEGRAM_EDIT_THROTTLE;

            if done {
                logging::info("rendered final telegram edit");
                return Ok(());
            }
        }
    }
}

fn should_exit_after_poll_conflicts(consecutive: u32) -> bool {
    consecutive >= 3
}

fn render_status(snapshot: &SessionSnapshot, config: &Config) -> String {
    let mut text = String::new();
    let _ = writeln!(text, "Current status");
    let _ = writeln!(
        text,
        "cwd: {}",
        snapshot
            .active_cwd
            .as_deref()
            .unwrap_or(config.codex_cwd.as_path())
            .display()
    );
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
    if !snapshot.cwd_history.is_empty() {
        let _ = writeln!(text, "cwd_history:");
        for (index, path) in snapshot.cwd_history.iter().enumerate() {
            let _ = writeln!(text, "  {index}: {}", path.display());
        }
    }
    text.trim_end().to_string()
}

fn render_cwd_history(snapshot: &SessionSnapshot) -> String {
    let mut text = String::new();
    let current = snapshot
        .active_cwd
        .as_deref()
        .map(Path::display)
        .map(|path| path.to_string())
        .unwrap_or_else(|| "(none)".to_string());
    let _ = writeln!(text, "Current working directory");
    let _ = writeln!(text, "{current}");
    if !snapshot.cwd_history.is_empty() {
        let _ = writeln!(text);
        let _ = writeln!(text, "History");
        for (index, path) in snapshot.cwd_history.iter().enumerate() {
            let _ = writeln!(text, "{index}: {}", path.display());
        }
    }
    text.trim_end().to_string()
}

fn load_local_codex_cwd_history() -> Result<Vec<PathBuf>> {
    let Some(codex_home) = codex_home_dir() else {
        return Ok(Vec::new());
    };

    let mut sessions = Vec::new();
    collect_rollout_files(&codex_home.join("sessions"), &mut sessions)?;
    collect_rollout_files(&codex_home.join("archived_sessions"), &mut sessions)?;
    sessions.sort_by(|left, right| right.cmp(left));

    let mut history = Vec::new();
    for path in sessions {
        if let Some(cwd) = read_session_cwd(&path)? {
            if cwd.is_dir() && !history.iter().any(|entry| entry == &cwd) {
                history.push(cwd);
            }
        }
    }
    Ok(history)
}

fn codex_home_dir() -> Option<PathBuf> {
    std::env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".codex")))
        .or_else(|| std::env::var_os("USERPROFILE").map(|home| PathBuf::from(home).join(".codex")))
}

fn collect_rollout_files(root: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    if !root.is_dir() {
        return Ok(());
    }

    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            collect_rollout_files(&path, files)?;
            continue;
        }
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("rollout-") && name.ends_with(".jsonl"))
        {
            files.push(path);
        }
    }
    Ok(())
}

fn read_session_cwd(path: &Path) -> Result<Option<PathBuf>> {
    let file = fs::File::open(path)?;
    let mut lines = BufReader::new(file).lines();
    let Some(first_line) = lines.next().transpose()? else {
        return Ok(None);
    };

    let record: serde_json::Value = serde_json::from_str(&first_line)?;
    Ok(record
        .get("payload")
        .and_then(|payload| payload.get("cwd"))
        .and_then(serde_json::Value::as_str)
        .map(PathBuf::from))
}

impl App {
    async fn resolve_requested_cwd(&self, input: &str) -> Result<PathBuf> {
        if let Ok(index) = input.parse::<usize>() {
            return self
                .session
                .resolve_cwd_history_entry(index)
                .await
                .ok_or_else(|| anyhow!("No working directory found at history index {index}."));
        }

        let path = PathBuf::from(input);
        let normalized = if path.is_absolute() {
            path
        } else {
            let base = self
                .session
                .active_cwd()
                .await
                .unwrap_or_else(|| self.config.codex_cwd.clone());
            base.join(path)
        };

        if !normalized.is_dir() {
            return Err(anyhow!(
                "Working directory does not exist or is not a directory:\n{}",
                normalized.display()
            ));
        }
        Ok(normalized)
    }
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
    use tempfile::tempdir;

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
        session.set_active_cwd(PathBuf::from("C:/work")).await;
        let snapshot = session.snapshot().await;
        let text = render_status(&snapshot, &sample_config());
        assert!(text.contains("thread-123"));
    }

    #[tokio::test]
    async fn render_cwd_history_lists_indexes() {
        let session = SharedSessionState::default();
        session.set_active_cwd(PathBuf::from("C:/one")).await;
        session.set_active_cwd(PathBuf::from("C:/two")).await;
        let snapshot = session.snapshot().await;
        let text = render_cwd_history(&snapshot);
        assert!(text.contains("0: C:/two"));
        assert!(text.contains("1: C:/one"));
    }

    #[test]
    fn loads_local_codex_cwd_history_from_rollouts() {
        let temp = tempdir().expect("tempdir");
        let sessions_root = temp
            .path()
            .join("sessions")
            .join("2026")
            .join("03")
            .join("27");
        fs::create_dir_all(&sessions_root).expect("create sessions dir");
        let older_cwd = temp.path().join("older");
        let newer_cwd = temp.path().join("newer");
        fs::create_dir_all(&older_cwd).expect("create older cwd");
        fs::create_dir_all(&newer_cwd).expect("create newer cwd");

        fs::write(
            sessions_root.join("rollout-2026-03-27T07-31-08-older.jsonl"),
            serde_json::json!({
                "timestamp": "2026-03-27T07:31:08.611Z",
                "type": "session_meta",
                "payload": { "cwd": older_cwd.display().to_string() }
            })
            .to_string()
                + "\n",
        )
        .expect("write older");
        fs::write(
            sessions_root.join("rollout-2026-03-27T08-31-08-newer.jsonl"),
            serde_json::json!({
                "timestamp": "2026-03-27T08:31:08.611Z",
                "type": "session_meta",
                "payload": { "cwd": newer_cwd.display().to_string() }
            })
            .to_string()
                + "\n",
        )
        .expect("write newer");
        fs::write(
            sessions_root.join("rollout-2026-03-27T09-31-08-duplicate.jsonl"),
            serde_json::json!({
                "timestamp": "2026-03-27T09:31:08.611Z",
                "type": "session_meta",
                "payload": { "cwd": newer_cwd.display().to_string() }
            })
            .to_string()
                + "\n",
        )
        .expect("write duplicate");

        let files = {
            let mut files = Vec::new();
            collect_rollout_files(temp.path().join("sessions").as_path(), &mut files)
                .expect("collect rollout files");
            files.sort_by(|left, right| right.cmp(left));
            files
        };

        let mut history = Vec::new();
        for path in files {
            if let Some(cwd) = read_session_cwd(&path).expect("read session cwd") {
                if !history.iter().any(|entry| entry == &cwd) {
                    history.push(cwd);
                }
            }
        }
        assert_eq!(history, vec![newer_cwd, older_cwd]);
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
