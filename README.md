# telegram-codex-bridge

A Rust Telegram bot that bridges a private Telegram chat to your local `codex app-server`.

This project is an MVP focused on one simple flow:

- start a Codex thread from Telegram
- send plain text prompts from Telegram
- stream Codex output back by editing the same Telegram message
- approve or deny Codex approval requests from Telegram
- stop a running turn from Telegram

## Features

- Rust implementation using `tokio`, `reqwest`, `serde`, and `toml`
- Telegram Bot API long polling
- Local `codex app-server` subprocess over stdio JSON-RPC
- Commands:
  - `/new`
  - `/use <thread_id>`
  - `/status`
  - `/stop`
  - `/approve [session]`
  - `/deny`
- In-memory session state
- Telegram approval workflow for command execution and file-change requests
- Configurable Codex sandbox mode

## Current Scope

This is intentionally limited:

- private chats only
- text input only
- no attachments
- no database persistence
- no group or forum topic support

## Requirements

- Rust toolchain
- `codex` CLI installed and available on `PATH`, or configured explicitly
- a Telegram bot token from BotFather

## Configuration

The app reads configuration from environment variables and optionally from `bridge.toml` in the project root.

Required:

- `TELEGRAM_BOT_TOKEN`
- `CODEX_CWD`

Optional:

- `TELEGRAM_ALLOWED_USER_ID`
  - if omitted, any Telegram user is allowed
- `CODEX_BINARY`
  - default: `codex`
- `CODEX_MODEL`
- `CODEX_APPROVAL_POLICY`
  - default: `never`
- `CODEX_SANDBOX_MODE`
  - supported values: `read-only`, `workspace-write`, `danger-full-access`
- `LOG_PATH`
  - default: `bridge.log`
- `LOCK_PATH`
  - default: `bridge.lock`
- `POLL_TIMEOUT_SECONDS`
  - default: `30`
- `UPDATE_LIMIT`
  - default: `50`

Example config:

```toml
telegram_bot_token = "123456:replace-me"
codex_cwd = 'C:\Users\you\workspace'

# Optional
# telegram_allowed_user_id = 123456789
# codex_binary = "codex"
# codex_model = "gpt-5.4"
# codex_approval_policy = "never"
# codex_sandbox_mode = "workspace-write"
# log_path = "bridge.log"
# lock_path = "bridge.lock"
# poll_timeout_seconds = 30
# update_limit = 50
```

Notes:

- On Windows, prefer TOML literal strings for paths:
  - `codex_cwd = 'C:\Users\you\workspace'`
- `CODEX_CWD` must already exist.
- To approve actions from Telegram, set `CODEX_APPROVAL_POLICY` or `codex_approval_policy` to `on-request`, `on-failure`, or `untrusted`.

## Run

Development:

```bash
cargo run
```

Release build:

```bash
cargo build --release
.\target\release\telegram-codex-bridge.exe
```

## Telegram Commands

- `/new`
  - create a new Codex thread and make it active
- `/use <thread_id>`
  - switch to an existing thread id
- `/status`
  - show current thread, turn state, cwd, model, and approval policy
- `/stop`
  - interrupt the current running turn
- `/approve`
  - approve the current pending request once
- `/approve session`
  - approve the current pending request for the current Codex session when supported
- `/deny`
  - decline the current pending request

Any non-command text message is sent as a `turn/start` request to the current active thread.

If there is no active thread yet, the bot tells you to run `/new`.

## Development

Format and test:

```bash
cargo fmt
cargo test
```

Build release binary:

```bash
cargo build --release
```

## Release

Latest GitHub release:

- [`v0.1.1`](https://github.com/winkar/codexchannel/releases/tag/v0.1.1)
