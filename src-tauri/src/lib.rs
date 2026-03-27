mod app;
mod codex;
mod commands;
mod config;
mod logging;
mod singleton;
mod state;
mod telegram;
mod tray;

pub use app::App;
pub use config::Config;
pub use state::SharedSessionState;

use serde::Serialize;
use tauri::{AppHandle, Manager, State};
use tauri_plugin_autostart::ManagerExt;

// ── Serialisable DTOs ────────────────────────────────────────────────────────

#[derive(Serialize, Clone)]
pub struct StatusDto {
    pub active_thread_id: Option<String>,
    pub active_turn_id: Option<String>,
    pub active_turn_running: bool,
    pub active_cwd: Option<String>,
    pub cwd_history: Vec<String>,
    pub pending_approval_message: Option<String>,
    pub pending_approval_supports_session: bool,
}

#[derive(Serialize, Clone)]
pub struct ConfigDto {
    pub telegram_bot_token_masked: String,
    pub telegram_allowed_user_id: Option<i64>,
    pub codex_binary: String,
    pub codex_cwd: String,
    pub codex_model: Option<String>,
    pub codex_approval_policy: String,
    pub codex_sandbox_mode: Option<String>,
}

// ── Tauri commands ───────────────────────────────────────────────────────────

#[tauri::command]
async fn get_status(session: State<'_, SharedSessionState>) -> Result<StatusDto, String> {
    let snap = session.snapshot().await;
    Ok(StatusDto {
        active_thread_id: snap.active_thread_id,
        active_turn_id: snap.active_turn_id,
        active_turn_running: snap.active_turn_running,
        active_cwd: snap.active_cwd.map(|p| p.display().to_string()),
        cwd_history: snap
            .cwd_history
            .iter()
            .map(|p| p.display().to_string())
            .collect(),
        pending_approval_message: snap.pending_approval_message,
        pending_approval_supports_session: snap.pending_approval_supports_session,
    })
}

#[tauri::command]
async fn get_config(config: State<'_, Config>) -> Result<ConfigDto, String> {
    let token = &config.telegram_bot_token;
    let masked = if token.len() > 8 {
        format!("{}…{}", &token[..4], &token[token.len() - 4..])
    } else {
        "****".to_string()
    };
    Ok(ConfigDto {
        telegram_bot_token_masked: masked,
        telegram_allowed_user_id: config.telegram_allowed_user_id,
        codex_binary: config.codex_binary.display().to_string(),
        codex_cwd: config.codex_cwd.display().to_string(),
        codex_model: config.codex_model.clone(),
        codex_approval_policy: config.codex_approval_policy.clone(),
        codex_sandbox_mode: config.codex_sandbox_mode.clone(),
    })
}

#[tauri::command]
async fn get_autostart_enabled(app: AppHandle) -> Result<bool, String> {
    app.autolaunch().is_enabled().map_err(|e| e.to_string())
}

#[tauri::command]
async fn set_autostart_enabled(app: AppHandle, enabled: bool) -> Result<(), String> {
    let al = app.autolaunch();
    if enabled {
        al.enable().map_err(|e| e.to_string())
    } else {
        al.disable().map_err(|e| e.to_string())
    }
}

// ── App entry point ──────────────────────────────────────────────────────────

pub fn run() {
    // Load configuration; if it fails the GUI still opens showing the error.
    let config_result = Config::load();

    // Initialise logging when config is available.
    let mut should_exit = false;
    if let Ok(ref cfg) = config_result {
        let _ = logging::init(&cfg.log_path);
        logging::info("starting telegram-codex-bridge (tauri)");
        // Enforce single-instance mode before creating any window or tray icon.
        if let Err(e) = singleton::acquire(&cfg.lock_path) {
            logging::error(&format!("failed to acquire singleton lock: {e:#}"));
            should_exit = true;
        }
    }

    if should_exit {
        return;
    }

    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_autostart::init(
            tauri_plugin_autostart::MacosLauncher::LaunchAgent,
            Some(vec!["--minimized"]),
        ))
        .setup(move |tauri_app| {
            // Build system tray icon + menu
            tray::setup_tray(tauri_app)?;

            // Intercept close event → hide to tray instead of quitting
            if let Some(window) = tauri_app.get_webview_window("main") {
                let win = window.clone();
                window.on_window_event(move |event| {
                    if let tauri::WindowEvent::CloseRequested { api, .. } = event {
                        api.prevent_close();
                        win.hide().ok();
                    }
                });
            }

            match config_result {
                Ok(config) => {
                    let app_instance =
                        App::new(config.clone()).expect("Failed to initialise bridge App");

                    // Register shared state so Tauri commands can read it
                    tauri_app.manage(app_instance.session.clone());
                    tauri_app.manage(config);

                    // Spawn the Telegram long-polling loop on Tauri's async runtime.
                    tauri::async_runtime::spawn(async move {
                        if let Err(e) = app_instance.run().await {
                            logging::error(&format!("Bridge loop terminated: {e:#}"));
                        }
                    });
                }
                Err(e) => {
                    logging::error(&format!("Config load failed: {e:#}"));
                    // Manage an empty session so commands don't panic
                    tauri_app.manage(SharedSessionState::default());
                }
            }

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            get_status,
            get_config,
            get_autostart_enabled,
            set_autostart_enabled,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
