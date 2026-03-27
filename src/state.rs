use std::sync::Arc;
use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Default)]
pub struct SessionSnapshot {
    pub active_thread_id: Option<String>,
    pub active_turn_id: Option<String>,
    pub active_turn_running: bool,
}

#[derive(Default)]
pub struct SessionState {
    active_thread_id: Option<String>,
    active_turn_id: Option<String>,
    active_turn_cancel: Option<CancellationToken>,
    active_turn_task: Option<JoinHandle<()>>,
}

#[derive(Clone, Default)]
pub struct SharedSessionState {
    inner: Arc<Mutex<SessionState>>,
}

impl SharedSessionState {
    pub async fn snapshot(&self) -> SessionSnapshot {
        let guard = self.inner.lock().await;
        SessionSnapshot {
            active_thread_id: guard.active_thread_id.clone(),
            active_turn_id: guard.active_turn_id.clone(),
            active_turn_running: guard.active_turn_cancel.is_some(),
        }
    }

    pub async fn active_thread_id(&self) -> Option<String> {
        self.inner.lock().await.active_thread_id.clone()
    }

    pub async fn set_active_thread(&self, thread_id: String) {
        self.inner.lock().await.active_thread_id = Some(thread_id);
    }

    pub async fn set_active_turn_id(&self, turn_id: String) {
        self.inner.lock().await.active_turn_id = Some(turn_id);
    }

    pub async fn set_active_turn(&self, cancel: CancellationToken, task: JoinHandle<()>) {
        let mut guard = self.inner.lock().await;
        guard.active_turn_cancel = Some(cancel);
        guard.active_turn_task = Some(task);
        guard.active_turn_id = None;
    }

    pub async fn clear_active_turn(&self) {
        let mut guard = self.inner.lock().await;
        guard.active_turn_cancel = None;
        guard.active_turn_task = None;
        guard.active_turn_id = None;
    }

    pub async fn cancel_active_turn(&self) -> bool {
        let mut guard = self.inner.lock().await;
        if let Some(cancel) = guard.active_turn_cancel.take() {
            cancel.cancel();
            true
        } else {
            false
        }
    }

    pub async fn has_active_turn(&self) -> bool {
        self.inner.lock().await.active_turn_cancel.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn set_and_clear_turn() {
        let state = SharedSessionState::default();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(async {});
        state.set_active_turn(cancel, task).await;
        assert!(state.has_active_turn().await);
        state.clear_active_turn().await;
        assert!(!state.has_active_turn().await);
    }

    #[tokio::test]
    async fn cancel_marks_running_turn() {
        let state = SharedSessionState::default();
        let cancel = CancellationToken::new();
        let child = cancel.clone();
        let task = tokio::spawn(async move {
            child.cancelled().await;
        });
        state.set_active_turn(cancel, task).await;
        assert!(state.cancel_active_turn().await);
    }
}
