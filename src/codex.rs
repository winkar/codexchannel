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
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct CodexClient {
    binary: PathBuf,
    cwd: PathBuf,
    model: Option<String>,
    approval_policy: String,
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
    stderr: Arc<Mutex<String>>,
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
        }
    }

    pub async fn start_thread(&self) -> Result<String> {
        let mut process = AppServerProcess::spawn(&self.binary).await?;
        process.initialize().await?;
        let request_id = process
            .send_request(
                "thread/start",
                json!({
                    "cwd": self.cwd.display().to_string(),
                    "model": self.model,
                    "approvalPolicy": self.approval_policy,
                    "experimentalRawEvents": false,
                    "persistExtendedHistory": false
                }),
            )
            .await?;
        let response = process.await_response(request_id).await?;
        let thread_id = response
            .get("thread")
            .and_then(|thread| thread.get("id"))
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("thread/start response missing thread id"))?
            .to_string();
        process.shutdown().await?;
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
        let mut process = AppServerProcess::spawn(&self.binary).await?;
        process.initialize().await?;
        let request_id = process
            .send_request(
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
            .await?;

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
                        let _ = process.send_request(
                            "turn/interrupt",
                            json!({"threadId": thread_id, "turnId": turn_id}),
                        ).await?;
                    }
                }
                next = process.next_message() => {
                    let Some(message) = next? else {
                        break;
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
                            handle_server_request(&mut process, id, &method, &params, &mut on_event).await?;
                        }
                    }
                }
            }
        }

        process.shutdown().await?;
        Ok(TurnRunResult {
            assistant_text,
            interrupted,
            last_status,
        })
    }
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
        let stderr_buffer = Arc::new(Mutex::new(String::new()));
        let stderr_clone = stderr_buffer.clone();
        let stderr_task = tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let mut buffer = stderr_clone.lock().await;
                if !buffer.is_empty() {
                    buffer.push('\n');
                }
                buffer.push_str(&line);
            }
        });
        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout).lines(),
            stderr: stderr_buffer,
            stderr_task,
            next_id: 1,
        })
    }

    async fn initialize(&mut self) -> Result<()> {
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
        Ok(())
    }

    async fn send_request(&mut self, method: &str, params: Value) -> Result<u64> {
        let id = self.next_id;
        self.next_id += 1;
        self.write_line(&json!({"method": method, "id": id, "params": params}))
            .await?;
        Ok(id)
    }

    async fn send_result(&mut self, id: u64, result: Value) -> Result<()> {
        self.write_line(&json!({"id": id, "result": result})).await
    }

    async fn send_notification(&mut self, method: &str) -> Result<()> {
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

    async fn shutdown(mut self) -> Result<()> {
        terminate_child(&mut self.child).await;
        let _ = self.stderr_task.await;
        let stderr = self.stderr.lock().await;
        if !stderr.trim().is_empty() {
            eprintln!("codex stderr:\n{}", stderr.trim());
        }
        Ok(())
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

async fn terminate_child(child: &mut Child) {
    match child.try_wait() {
        Ok(Some(_)) => {}
        Ok(None) => {
            let _ = child.kill().await;
        }
        Err(_) => {}
    }
    let _ = child.wait().await;
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
}
