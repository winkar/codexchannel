use crate::logging;
use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::Mutex,
    time::{Duration, timeout},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct CodexClient {
    binary: PathBuf,
    cwd: PathBuf,
    model: Option<String>,
    approval_policy: String,
    process: Arc<Mutex<Option<AppServerProcess>>>,
}

#[derive(Debug, Clone)]
pub enum TurnEvent {
    ThreadReady(String),
    TurnStarted(String),
    AssistantDelta(String),
    Status(String),
}

#[derive(Debug, Clone)]
pub struct TurnRunResult {
    pub assistant_text: String,
    pub interrupted: bool,
    pub last_status: Option<String>,
}

pub struct AppServerProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: tokio::io::Lines<BufReader<ChildStdout>>,
    stderr_task: tokio::task::JoinHandle<()>,
    next_id: u64,
}

#[derive(Debug, Clone)]
enum RpcMessage {
    Response {
        id: u64,
        result: Option<Value>,
        error: Option<RpcError>,
    },
    Notification {
        method: String,
        params: Value,
    },
    ServerRequest {
        id: u64,
        method: String,
        params: Value,
    },
}

#[derive(Debug, Clone, serde::Deserialize)]
struct RpcError {
    message: String,
    #[serde(default)]
    code: Option<i64>,
    #[serde(default)]
    data: Option<Value>,
}

impl CodexClient {
    pub fn new(
        binary: PathBuf,
        cwd: PathBuf,
        model: Option<String>,
        approval_policy: String,
    ) -> Self {
        Self {
            binary,
            cwd,
            model,
            approval_policy,
            process: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn start_thread(&self) -> Result<String> {
        logging::info(&format!(
            "codex start_thread begin binary={} cwd={}",
            self.binary.display(),
            self.cwd.display()
        ));
        let mut process_guard = self.process.lock().await;
        ensure_process(
            &mut process_guard,
            &self.binary,
            Duration::from_secs(10),
        )
        .await?;
        let request_id = match send_request_with_recovery(
            &mut process_guard,
            "thread/start",
            json!({
                "cwd": self.cwd.display().to_string(),
                "model": self.model,
                "approvalPolicy": self.approval_policy,
                "experimentalRawEvents": false,
                "persistExtendedHistory": false
            }),
        )
        .await
        {
            Ok(request_id) => request_id,
            Err(error) => {
                reset_process_after_transport_error(&mut process_guard, &error, "thread/start request");
                return Err(error);
            }
        };
        let response = match await_response_with_timeout_and_recovery(
            &mut process_guard,
            request_id,
            Duration::from_secs(45),
            "timed out waiting for codex thread/start",
        )
        .await
        {
            Ok(response) => response,
            Err(error) => {
                reset_process_after_transport_error(&mut process_guard, &error, "thread/start response");
                return Err(error);
            }
        };
        let thread_id = response
            .get("thread")
            .and_then(|thread| thread.get("id"))
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("thread/start response missing thread id"))?
            .to_string();
        logging::info(&format!("codex start_thread got thread_id={thread_id}"));
        Ok(thread_id)
    }

    pub async fn run_turn<F, Fut>(
        &self,
        thread_id: &str,
        prompt: &str,
        cancel: CancellationToken,
        mut on_event: F,
    ) -> Result<TurnRunResult>
    where
        F: FnMut(TurnEvent) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        logging::info(&format!(
            "codex run_turn begin thread_id={} cwd={}",
            thread_id,
            self.cwd.display()
        ));
        let mut process_guard = self.process.lock().await;
        ensure_process(
            &mut process_guard,
            &self.binary,
            Duration::from_secs(10),
        )
        .await?;
        let request_id = match send_request_with_recovery(
            &mut process_guard,
            "turn/start",
            json!({
                "threadId": thread_id,
                "cwd": self.cwd.display().to_string(),
                "approvalPolicy": self.approval_policy,
                "model": self.model,
                "input": [
                    {
                        "type": "text",
                        "text": prompt,
                        "text_elements": []
                    }
                ]
            }),
        )
        .await
        {
            Ok(request_id) => request_id,
            Err(error) => {
                reset_process_after_transport_error(&mut process_guard, &error, "turn/start request");
                return Err(error);
            }
        };

        let mut assistant_text = String::new();
        let mut interrupted = false;
        let mut turn_started = None::<String>;
        let mut interrupt_sent = false;
        let mut done = false;
        let mut last_status = None::<String>;

        while !done {
            tokio::select! {
                _ = cancel.cancelled(), if !interrupt_sent => {
                    interrupt_sent = true;
                    interrupted = true;
                    if let Some(turn_id) = turn_started.as_deref() {
                        let interrupt_result = send_request_with_recovery(
                            &mut process_guard,
                            "turn/interrupt",
                            json!({"threadId": thread_id, "turnId": turn_id}),
                        )
                        .await;
                        if let Err(error) = interrupt_result {
                            reset_process_after_transport_error(&mut process_guard, &error, "turn/interrupt");
                            return Err(error);
                        }
                    }
                }
                next = next_message_with_recovery(&mut process_guard) => {
                    let message = match next {
                        Ok(message) => message,
                        Err(error) => {
                            reset_process_after_transport_error(&mut process_guard, &error, "turn stream");
                            return Err(error);
                        }
                    };
                    match message {
                        RpcMessage::Response { id, result, error } => {
                            if id == request_id {
                                if let Some(error) = error {
                                    bail!("{}", format_rpc_error(&error));
                                }
                                if let Some(turn_id) = result
                                    .as_ref()
                                    .and_then(|value| value.get("turn"))
                                    .and_then(|turn| turn.get("id"))
                                    .and_then(Value::as_str)
                                {
                                    turn_started = Some(turn_id.to_string());
                                    on_event(TurnEvent::TurnStarted(turn_id.to_string())).await?;
                                }
                            }
                        }
                        RpcMessage::Notification { method, params } => {
                            handle_notification(
                                &method,
                                &params,
                                &mut assistant_text,
                                &mut turn_started,
                                &mut done,
                                &mut interrupted,
                                &mut last_status,
                                &mut on_event,
                            ).await?;
                        }
                        RpcMessage::ServerRequest { id, method, params } => {
                            let server_request = {
                                let process = process_guard.as_mut().ok_or_else(|| {
                                    anyhow!("codex app-server unavailable while handling server request")
                                })?;
                                handle_server_request(process, id, &method, &params, &mut on_event).await
                            };
                            if let Err(error) = server_request {
                                reset_process_after_transport_error(&mut process_guard, &error, &format!("server request {method}"));
                                return Err(error);
                            }
                        }
                    }
                }
            }
        }

        logging::info(&format!(
            "codex run_turn done thread_id={} interrupted={}",
            thread_id, interrupted
        ));
        Ok(TurnRunResult {
            assistant_text,
            interrupted,
            last_status,
        })
    }
}

async fn ensure_process(
    process_guard: &mut Option<AppServerProcess>,
    binary: &Path,
    initialize_timeout: Duration,
) -> Result<()> {
    if process_guard.is_some() {
        logging::info("reusing existing codex app-server");
        return Ok(());
    }
    let mut process = AppServerProcess::spawn(binary).await?;
    let initialize_result = timeout(initialize_timeout, process.initialize()).await;
    match initialize_result {
        Ok(Ok(())) => {
            logging::info("codex app-server initialized");
        }
        Ok(Err(error)) => {
            logging::error(&format!("codex initialize failed: {error:#}"));
            process.abort();
            return Err(error);
        }
        Err(error) => {
            logging::error(&format!("codex initialize timed out: {error}"));
            process.abort();
            return Err(anyhow!("timed out waiting for codex initialize"));
        }
    }
    *process_guard = Some(process);
    Ok(())
}

async fn send_request_with_recovery(
    process_guard: &mut Option<AppServerProcess>,
    method: &str,
    params: Value,
) -> Result<u64> {
    let process = process_guard
        .as_mut()
        .ok_or_else(|| anyhow!("codex app-server unavailable before request"))?;
    process.send_request(method, params).await
}

async fn await_response_with_timeout_and_recovery(
    process_guard: &mut Option<AppServerProcess>,
    expected_id: u64,
    wait_timeout: Duration,
    timeout_message: &str,
) -> Result<Value> {
    let process = process_guard
        .as_mut()
        .ok_or_else(|| anyhow!("codex app-server unavailable before awaiting response"))?;
    timeout(wait_timeout, process.await_response(expected_id))
        .await
        .map_err(|_| anyhow!(timeout_message.to_string()))?
}

async fn next_message_with_recovery(process_guard: &mut Option<AppServerProcess>) -> Result<RpcMessage> {
    let process = process_guard
        .as_mut()
        .ok_or_else(|| anyhow!("codex app-server unavailable before reading next message"))?;
    match process.next_message().await? {
        Some(message) => Ok(message),
        None => Err(anyhow!("app-server closed during active turn")),
    }
}

fn is_transport_error(error: &anyhow::Error) -> bool {
    let text = format!("{error:#}").to_ascii_lowercase();
    [
        "failed reading app-server stdout",
        "failed writing app-server request",
        "failed flushing stdin",
        "app-server closed before response",
        "app-server closed during active turn",
        "timed out waiting for codex initialize",
        "timed out waiting for codex thread/start",
        "broken pipe",
        "pipe has been ended",
        "channel closed",
    ]
    .iter()
    .any(|needle| text.contains(needle))
}

fn reset_slot_on_transport_error<T>(slot: &mut Option<T>, error: &anyhow::Error) -> bool {
    if !is_transport_error(error) {
        return false;
    }
    *slot = None;
    true
}

fn reset_process_after_transport_error(
    process_guard: &mut Option<AppServerProcess>,
    error: &anyhow::Error,
    context: &str,
) {
    if !is_transport_error(error) {
        return;
    }
    if let Some(mut process) = process_guard.take() {
        logging::error(&format!(
            "resetting codex app-server after transport error in {context}: {error:#}"
        ));
        process.abort();
        return;
    }
    let _ = reset_slot_on_transport_error(process_guard, error);
}

async fn handle_notification<F, Fut>(
    method: &str,
    params: &Value,
    assistant_text: &mut String,
    turn_started: &mut Option<String>,
    done: &mut bool,
    interrupted: &mut bool,
    last_status: &mut Option<String>,
    on_event: &mut F,
) -> Result<()>
where
    F: FnMut(TurnEvent) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    match method {
        "thread/started" => {
            if let Some(thread_id) = params
                .get("thread")
                .and_then(|thread| thread.get("id"))
                .and_then(Value::as_str)
            {
                on_event(TurnEvent::ThreadReady(thread_id.to_string())).await?;
            }
        }
        "turn/started" => {
            if let Some(turn_id) = params
                .get("turn")
                .and_then(|turn| turn.get("id"))
                .and_then(Value::as_str)
            {
                *turn_started = Some(turn_id.to_string());
                on_event(TurnEvent::TurnStarted(turn_id.to_string())).await?;
            }
        }
        "item/agentMessage/delta" => {
            if let Some(delta) = params.get("delta").and_then(Value::as_str) {
                assistant_text.push_str(delta);
                on_event(TurnEvent::AssistantDelta(assistant_text.clone())).await?;
            }
        }
        "item/completed" => {
            if let Some(item) = params.get("item") {
                if item.get("type").and_then(Value::as_str) == Some("agentMessage") {
                    if let Some(text) = item.get("text").and_then(Value::as_str) {
                        *assistant_text = text.to_string();
                        on_event(TurnEvent::AssistantDelta(assistant_text.clone())).await?;
                    }
                } else if item.get("type").and_then(Value::as_str) == Some("commandExecution") {
                    let command = item
                        .get("command")
                        .and_then(Value::as_str)
                        .unwrap_or("command");
                    let status = item
                        .get("status")
                        .and_then(Value::as_str)
                        .unwrap_or("completed");
                    *last_status = Some(format!("{command}: {status}"));
                    on_event(TurnEvent::Status(last_status.clone().unwrap_or_default())).await?;
                }
            }
        }
        "turn/completed" => {
            if let Some(turn) = params.get("turn") {
                if turn.get("status").and_then(Value::as_str) == Some("interrupted") {
                    *interrupted = true;
                }
            }
            *done = true;
        }
        _ => {}
    }
    Ok(())
}

async fn handle_server_request<F, Fut>(
    process: &mut AppServerProcess,
    id: u64,
    method: &str,
    _params: &Value,
    on_event: &mut F,
) -> Result<()>
where
    F: FnMut(TurnEvent) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    match method {
        "item/commandExecution/requestApproval" => {
            process
                .send_result(id, json!({"decision":"decline"}))
                .await?;
            on_event(TurnEvent::Status(
                "Codex requested command approval and it was declined.".to_string(),
            ))
            .await?;
        }
        "item/fileChange/requestApproval" => {
            process
                .send_result(id, json!({"decision":"decline"}))
                .await?;
            on_event(TurnEvent::Status(
                "Codex requested file-change approval and it was declined.".to_string(),
            ))
            .await?;
        }
        "item/tool/requestUserInput" => {
            process.send_result(id, json!({"answers": {}})).await?;
            on_event(TurnEvent::Status(
                "Codex requested extra user input; this MVP does not support it.".to_string(),
            ))
            .await?;
        }
        _ => {
            if process
                .send_result(id, json!({"decision":"decline"}))
                .await
                .is_err()
            {
                process.send_result(id, json!({})).await?;
            }
        }
    }
    Ok(())
}

impl AppServerProcess {
    async fn spawn(binary: &Path) -> Result<Self> {
        logging::info(&format!("spawning codex app-server via {}", binary.display()));
        let mut child = spawnable_command(binary)
            .spawn()
            .with_context(|| format!("failed to spawn `{}`", binary.display()))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("codex app-server stdin unavailable"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("codex app-server stdout unavailable"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow!("codex app-server stderr unavailable"))?;
        let stderr_task = tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                logging::error(&format!("codex stderr: {line}"));
            }
        });
        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout).lines(),
            stderr_task,
            next_id: 1,
        })
    }

    async fn initialize(&mut self) -> Result<()> {
        logging::info("sending codex initialize");
        let request_id = self
            .send_request(
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "telegram-codex-bridge",
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    "capabilities": {
                        "experimentalApi": false
                    }
                }),
            )
            .await?;
        let _ = self.await_response(request_id).await?;
        self.send_notification("initialized").await?;
        logging::info("codex initialize complete");
        Ok(())
    }

    async fn send_request(&mut self, method: &str, params: Value) -> Result<u64> {
        let id = self.next_id;
        self.next_id += 1;
        logging::info(&format!("codex request id={} method={method}", id));
        self.write_line(&json!({"method": method, "id": id, "params": params}))
            .await?;
        Ok(id)
    }

    async fn send_result(&mut self, id: u64, result: Value) -> Result<()> {
        logging::info(&format!("codex response id={id}"));
        self.write_line(&json!({"id": id, "result": result})).await
    }

    async fn send_notification(&mut self, method: &str) -> Result<()> {
        logging::info(&format!("codex notification method={method}"));
        self.write_line(&json!({"method": method})).await
    }

    async fn await_response(&mut self, expected_id: u64) -> Result<Value> {
        loop {
            let Some(message) = self.next_message().await? else {
                bail!("app-server closed before response {expected_id}");
            };
            if let RpcMessage::Response { id, result, error } = message {
                if id != expected_id {
                    continue;
                }
                if let Some(error) = error {
                    bail!("{}", format_rpc_error(&error));
                }
                return Ok(result.unwrap_or(Value::Null));
            }
        }
    }

    async fn next_message(&mut self) -> Result<Option<RpcMessage>> {
        loop {
            let Some(line) = self
                .stdout
                .next_line()
            .await
            .context("failed reading app-server stdout")?
            else {
                return Ok(None);
            };
            if let Some(message) = parse_rpc_message(&line)? {
                match &message {
                    RpcMessage::Response { id, .. } => {
                        logging::info(&format!("codex incoming response id={id}"));
                    }
                    RpcMessage::Notification { method, .. } => {
                        logging::info(&format!("codex incoming notification method={method}"));
                    }
                    RpcMessage::ServerRequest { id, method, .. } => {
                        logging::info(&format!(
                            "codex incoming server request id={} method={}",
                            id, method
                        ));
                    }
                }
                return Ok(Some(message));
            }
        }
    }

    async fn write_line(&mut self, value: &Value) -> Result<()> {
        let mut payload = serde_json::to_vec(value)?;
        payload.push(b'\n');
        self.stdin
            .write_all(&payload)
            .await
            .context("failed writing app-server request")?;
        self.stdin.flush().await.context("failed flushing stdin")?;
        Ok(())
    }

    fn abort(&mut self) {
        self.stderr_task.abort();
        let _ = self.child.start_kill();
    }
}

fn spawnable_command(binary: &Path) -> Command {
    let mut command = Command::new(binary);
    command
        .arg("app-server")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

fn format_rpc_error(error: &RpcError) -> String {
    let mut parts = vec![error.message.clone()];
    if let Some(code) = error.code {
        parts.push(format!("code {code}"));
    }
    if let Some(data) = &error.data {
        parts.push(data.to_string());
    }
    parts.join(" | ")
}

fn parse_rpc_message(line: &str) -> Result<Option<RpcMessage>> {
    let value: Value = match serde_json::from_str(line) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    let method = value
        .get("method")
        .and_then(Value::as_str)
        .map(str::to_string);
    let id = value.get("id").and_then(Value::as_u64);
    let params = value.get("params").cloned().unwrap_or(Value::Null);
    let result = value.get("result").cloned();
    let error = value
        .get("error")
        .cloned()
        .map(serde_json::from_value)
        .transpose()?;
    Ok(Some(match (method, id, result, error) {
        (Some(method), Some(id), _, _) => RpcMessage::ServerRequest { id, method, params },
        (Some(method), None, _, _) => RpcMessage::Notification { method, params },
        (None, Some(id), result, error) => RpcMessage::Response { id, result, error },
        _ => return Ok(None),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;

    #[test]
    fn parses_response_message() {
        let message = parse_rpc_message(r#"{"id":1,"result":{"ok":true}}"#)
            .expect("parse")
            .expect("message");
        match message {
            RpcMessage::Response { id, .. } => assert_eq!(id, 1),
            _ => panic!("expected response"),
        }
    }

    #[test]
    fn parses_notification_message() {
        let message =
            parse_rpc_message(r#"{"method":"turn/started","params":{"turn":{"id":"t1"}}}"#)
                .expect("parse")
                .expect("message");
        match message {
            RpcMessage::Notification { method, .. } => assert_eq!(method, "turn/started"),
            _ => panic!("expected notification"),
        }
    }

    #[tokio::test]
    async fn agent_delta_appends_text() {
        let mut assistant = String::new();
        let mut turn_started = None;
        let mut done = false;
        let mut interrupted = false;
        let mut last_status = None;
        handle_notification(
            "item/agentMessage/delta",
            &json!({"delta":"hello"}),
            &mut assistant,
            &mut turn_started,
            &mut done,
            &mut interrupted,
            &mut last_status,
            &mut |_| async { Ok(()) },
        )
        .await
        .expect("notification");
        assert_eq!(assistant, "hello");
    }

    #[test]
    fn transport_error_resets_slot() {
        let mut slot = Some(123u32);
        let error = anyhow!("failed writing app-server request: broken pipe");
        assert!(reset_slot_on_transport_error(&mut slot, &error));
        assert!(slot.is_none());
    }

    #[test]
    fn business_error_does_not_reset_slot() {
        let mut slot = Some(123u32);
        let error = anyhow!("thread not found: abc | code -32600");
        assert!(!reset_slot_on_transport_error(&mut slot, &error));
        assert_eq!(slot, Some(123u32));
    }
}
