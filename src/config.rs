use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

#[derive(Debug, Clone)]
pub struct Config {
    pub telegram_bot_token: String,
    pub telegram_allowed_user_id: Option<i64>,
    pub codex_binary: PathBuf,
    pub codex_cwd: PathBuf,
    pub log_path: PathBuf,
    pub lock_path: PathBuf,
    pub codex_model: Option<String>,
    pub codex_approval_policy: String,
    pub poll_timeout_seconds: u64,
    pub update_limit: u32,
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    telegram_bot_token: Option<String>,
    telegram_allowed_user_id: Option<i64>,
    codex_binary: Option<PathBuf>,
    codex_cwd: Option<PathBuf>,
    log_path: Option<PathBuf>,
    lock_path: Option<PathBuf>,
    codex_model: Option<String>,
    codex_approval_policy: Option<String>,
    poll_timeout_seconds: Option<u64>,
    update_limit: Option<u32>,
}

impl Config {
    pub fn load() -> Result<Self> {
        let file_config = load_file_config()?;
        let telegram_bot_token = read_string("TELEGRAM_BOT_TOKEN", file_config.telegram_bot_token)?;
        let telegram_allowed_user_id = read_optional_i64(
            "TELEGRAM_ALLOWED_USER_ID",
            file_config.telegram_allowed_user_id,
        )?;
        let codex_cwd = read_path("CODEX_CWD", file_config.codex_cwd)?;
        if !codex_cwd.is_dir() {
            return Err(anyhow!(
                "CODEX_CWD must point to an existing directory: {}",
                codex_cwd.display()
            ));
        }

        Ok(Self {
            telegram_bot_token,
            telegram_allowed_user_id,
            codex_binary: resolve_codex_binary(
                env::var_os("CODEX_BINARY")
                    .map(PathBuf::from)
                    .or(file_config.codex_binary)
                    .unwrap_or_else(|| PathBuf::from("codex")),
            ),
            codex_cwd,
            log_path: env::var_os("LOG_PATH")
                .map(PathBuf::from)
                .or(file_config.log_path)
                .unwrap_or_else(|| PathBuf::from("bridge.log")),
            lock_path: env::var_os("LOCK_PATH")
                .map(PathBuf::from)
                .or(file_config.lock_path)
                .unwrap_or_else(|| PathBuf::from("bridge.lock")),
            codex_model: env::var("CODEX_MODEL").ok().or(file_config.codex_model),
            codex_approval_policy: env::var("CODEX_APPROVAL_POLICY")
                .ok()
                .or(file_config.codex_approval_policy)
                .unwrap_or_else(|| "never".to_string()),
            poll_timeout_seconds: env::var("POLL_TIMEOUT_SECONDS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .or(file_config.poll_timeout_seconds)
                .unwrap_or(30),
            update_limit: env::var("UPDATE_LIMIT")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .or(file_config.update_limit)
                .unwrap_or(50),
        })
    }
}

fn load_file_config() -> Result<FileConfig> {
    let path = PathBuf::from("bridge.toml");
    if !path.exists() {
        return Ok(FileConfig::default());
    }
    let text =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    toml::from_str(&text).context("failed to parse bridge.toml")
}

fn read_string(name: &str, fallback: Option<String>) -> Result<String> {
    env::var(name)
        .ok()
        .or(fallback)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("{name} is required"))
}

fn read_optional_i64(name: &str, fallback: Option<i64>) -> Result<Option<i64>> {
    if let Ok(value) = env::var(name) {
        return value
            .parse::<i64>()
            .map(Some)
            .with_context(|| format!("{name} must be an integer"));
    }
    Ok(fallback)
}

fn read_path(name: &str, fallback: Option<PathBuf>) -> Result<PathBuf> {
    env::var_os(name)
        .map(PathBuf::from)
        .or(fallback)
        .ok_or_else(|| anyhow!("{name} is required"))
}

fn resolve_codex_binary(binary: PathBuf) -> PathBuf {
    #[cfg(windows)]
    {
        resolve_windows_command(binary)
    }

    #[cfg(not(windows))]
    {
        binary
    }
}

#[cfg(windows)]
fn resolve_windows_command(binary: PathBuf) -> PathBuf {
    if binary.components().count() > 1 || binary.extension().is_some() {
        return binary;
    }

    let Ok(output) = Command::new("where.exe").arg(&binary).output() else {
        return binary;
    };
    if !output.status.success() {
        return binary;
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut candidates: Vec<PathBuf> = stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(PathBuf::from)
        .collect();
    candidates.sort_by_key(|path| windows_command_rank(path));
    candidates.into_iter().next().unwrap_or(binary)
}

#[cfg(windows)]
fn windows_command_rank(path: &Path) -> (u8, String) {
    let extension = path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_ascii_lowercase());
    let is_windows_apps = path
        .to_string_lossy()
        .to_ascii_lowercase()
        .contains("\\windowsapps\\");

    let ext_rank = match extension.as_deref() {
        Some("cmd") => 0,
        Some("bat") => 1,
        Some("exe") => 2,
        Some("com") => 3,
        Some(_) => 4,
        None => 5,
    };
    let windows_apps_rank = if is_windows_apps { 1 } else { 0 };
    (ext_rank, format!("{windows_apps_rank}:{}", path.display()))
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;

    #[test]
    fn prefers_cmd_over_extensionless_and_windowsapps_exe() {
        let mut candidates = vec![
            PathBuf::from(r"C:\Users\winka\AppData\Roaming\npm\codex"),
            PathBuf::from(
                r"C:\Program Files\WindowsApps\OpenAI.Codex_26.313.5234.0_x64__2p2nqsd0c76g0\app\resources\codex.exe",
            ),
            PathBuf::from(r"C:\Users\winka\AppData\Roaming\npm\codex.cmd"),
        ];
        candidates.sort_by_key(|path| windows_command_rank(path));
        assert_eq!(
            candidates.first().unwrap(),
            &PathBuf::from(r"C:\Users\winka\AppData\Roaming\npm\codex.cmd")
        );
    }
}
