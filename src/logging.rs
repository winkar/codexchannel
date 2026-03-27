use anyhow::{Context, Result};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
    sync::{Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

static LOG_FILE: OnceLock<Mutex<File>> = OnceLock::new();

pub fn init(path: &Path) -> Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open log file {}", path.display()))?;
    let _ = LOG_FILE.set(Mutex::new(file));
    info(&format!("logging initialized at {}", path.display()));
    Ok(())
}

pub fn info(message: &str) {
    write_line("INFO", message);
}

pub fn error(message: &str) {
    write_line("ERROR", message);
}

fn write_line(level: &str, message: &str) {
    let line = format!("{} [{}] {}\n", unix_timestamp(), level, message);
    eprint!("{line}");
    if let Some(file) = LOG_FILE.get() {
        if let Ok(mut file) = file.lock() {
            let _ = file.write_all(line.as_bytes());
            let _ = file.flush();
        }
    }
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}
