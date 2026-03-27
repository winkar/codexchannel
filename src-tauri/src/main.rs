// Tauri requires the main function to be a plain sync fn.
// The tokio runtime is started internally by Tauri; we just delegate
// to the shared `lib::run()` entry point defined in lib.rs.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    telegram_codex_bridge::run();
}
