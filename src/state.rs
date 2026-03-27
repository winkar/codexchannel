use std::sync::Arc;
use tokio::{
    sync::{Mutex, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalDecision {
    Accept,
    AcceptForSession,
    Decline,
    Cancel,
}

#[derive(Debug)]
pub struct PendingApproval {
    pub message: String,
    pub allow_accept_for_session: bool,
    responder: oneshot::Sender<ApprovalDecision>,
}

impl PendingApproval {
    pub fn new(
        message: String,
        allow_accept_for_session: bool,
        responder: oneshot::Sender<ApprovalDecision>,
    ) -> Self {
        Self {
            message,
            allow_accept_for_session,
            responder,
        }
    }

    fn resolve(self, decision: ApprovalDecision) -> bool {
        self.responder.send(decision).is_ok()
    }
}

#[derive(Debug, Clone, Default)]
pub struct SessionSnapshot {
    pub active_thread_id: Option<String>,
    pub active_turn_id: Option<String>,
    pub active_turn_running: bool,
    pub pending_approval_message: Option<String>,
    pub pending_approval_supports_session: bool,
}

#[derive(Default)]
pub struct SessionState {
    active_thread_id: Option<String>,
    active_turn_id: Option<String>,
    active_turn_cancel: Option<CancellationToken>,
    active_turn_task: Option<JoinHandle<()>>,
    pending_approval: Option<PendingApproval>,
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
            pending_approval_message: guard
                .pending_approval
                .as_ref()
                .map(|approval| approval.message.clone()),
            pending_approval_supports_session: guard
                .pending_approval
                .as_ref()
                .map(|approval| approval.allow_accept_for_session)
                .unwrap_or(false),
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
        if let Some(approval) = guard.pending_approval.take() {
            let _ = approval.resolve(ApprovalDecision::Cancel);
        }
    }

    pub async fn cancel_active_turn(&self) -> bool {
        let mut guard = self.inner.lock().await;
        if let Some(approval) = guard.pending_approval.take() {
            let _ = approval.resolve(ApprovalDecision::Cancel);
        }
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

    pub async fn set_pending_approval(&self, approval: PendingApproval) {
        let mut guard = self.inner.lock().await;
        if let Some(existing) = guard.pending_approval.take() {
            let _ = existing.resolve(ApprovalDecision::Cancel);
        }
        guard.pending_approval = Some(approval);
    }

    pub async fn resolve_pending_approval(&self, decision: ApprovalDecision) -> bool {
        let mut guard = self.inner.lock().await;
        guard
            .pending_approval
            .take()
            .map(|approval| approval.resolve(decision))
            .unwrap_or(false)
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

    #[tokio::test]
    async fn resolves_pending_approval() {
        let state = SharedSessionState::default();
        let (sender, receiver) = oneshot::channel();
        state
            .set_pending_approval(PendingApproval::new("approve".to_string(), false, sender))
            .await;
        assert!(
            state
                .resolve_pending_approval(ApprovalDecision::Accept)
                .await
        );
        assert_eq!(receiver.await.expect("decision"), ApprovalDecision::Accept);
    }

    #[tokio::test]
    async fn replacing_pending_approval_cancels_previous() {
        let state = SharedSessionState::default();
        let (sender_one, receiver_one) = oneshot::channel();
        let (sender_two, receiver_two) = oneshot::channel();
        state
            .set_pending_approval(PendingApproval::new("first".to_string(), false, sender_one))
            .await;
        state
            .set_pending_approval(PendingApproval::new("second".to_string(), true, sender_two))
            .await;
        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.pending_approval_message.as_deref(), Some("second"));
        assert!(snapshot.pending_approval_supports_session);
        assert_eq!(
            receiver_one.await.expect("decision"),
            ApprovalDecision::Cancel
        );
        assert!(
            state
                .resolve_pending_approval(ApprovalDecision::AcceptForSession)
                .await
        );
        assert_eq!(
            receiver_two.await.expect("decision"),
            ApprovalDecision::AcceptForSession
        );
    }
}
