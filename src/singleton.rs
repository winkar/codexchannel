use anyhow::{Context, Result, bail};
use fs2::FileExt;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
    sync::OnceLock,
};

static LOCK_FILE: OnceLock<File> = OnceLock::new();

pub fn acquire(path: &Path) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("failed to open lock file {}", path.display()))?;

    if file.try_lock_exclusive().is_err() {
        bail!(
            "another telegram-codex-bridge instance is already running (lock: {})",
            path.display()
        );
    }

    file.set_len(0)
        .with_context(|| format!("failed to reset lock file {}", path.display()))?;
    writeln!(file, "pid={}", std::process::id())
        .with_context(|| format!("failed to write lock file {}", path.display()))?;

    let _ = LOCK_FILE.set(file);
    Ok(())
}
