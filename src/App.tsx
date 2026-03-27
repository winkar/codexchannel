import { useState, useEffect, useCallback } from "react";
import { invoke } from "@tauri-apps/api/core";
import { enable, disable, isEnabled } from "@tauri-apps/plugin-autostart";

// ── Types mirroring the Rust DTOs ────────────────────────────────────────────

interface StatusDto {
  active_thread_id: string | null;
  active_turn_id: string | null;
  active_turn_running: boolean;
  active_cwd: string | null;
  cwd_history: string[];
  pending_approval_message: string | null;
  pending_approval_supports_session: boolean;
}

interface ConfigDto {
  telegram_bot_token_masked: string;
  telegram_allowed_user_id: number | null;
  codex_binary: string;
  codex_cwd: string;
  codex_model: string | null;
  codex_approval_policy: string;
  codex_sandbox_mode: string | null;
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function truncate(s: string, max = 40): string {
  return s.length > max ? "…" + s.slice(s.length - max) : s;
}

// ── Sub-components ────────────────────────────────────────────────────────────

function InfoCard({
  label,
  value,
}: {
  label: string;
  value: string | null | undefined;
}) {
  return (
    <div className="card">
      <div className="card-label">{label}</div>
      {value ? (
        <div className="card-value">{value}</div>
      ) : (
        <div className="card-value muted">—</div>
      )}
    </div>
  );
}

function StatusPanel({
  status,
  error,
}: {
  status: StatusDto | null;
  error: string | null;
}) {
  if (error) {
    return (
      <div className="panel">
        <div className="panel-title">Status</div>
        <div className="error-box">
          <strong>Bridge unavailable</strong>
          {error}
        </div>
        <p style={{ fontSize: 12, color: "var(--text-dim)", lineHeight: 1.6 }}>
          Make sure <code>bridge.toml</code> is configured next to the
          executable, then restart the app.
        </p>
      </div>
    );
  }

  if (!status) {
    return (
      <div className="panel">
        <div className="panel-title">Status</div>
        {[1, 2, 3, 4].map((i) => (
          <div className="card" key={i} style={{ marginBottom: 12 }}>
            <div className="skeleton" style={{ width: "40%", marginBottom: 8 }} />
            <div className="skeleton" style={{ width: "70%" }} />
          </div>
        ))}
      </div>
    );
  }

  const turnBadge = status.active_turn_running ? (
    <span className="badge yellow">⚡ Running</span>
  ) : (
    <span className="badge green">● Idle</span>
  );

  return (
    <div className="panel">
      <div className="panel-title">Status</div>

      {status.pending_approval_message && (
        <div className="approval-banner">
          <strong>⚠ Approval requested</strong>
          {status.pending_approval_message}
        </div>
      )}

      {/* Turn state */}
      <div className="card">
        <div className="card-label">Turn state</div>
        <div className="card-value">{turnBadge}</div>
      </div>

      <InfoCard
        label="Active thread"
        value={status.active_thread_id}
      />

      {status.active_turn_id && (
        <InfoCard label="Turn ID" value={status.active_turn_id} />
      )}

      <InfoCard
        label="Working directory"
        value={status.active_cwd ?? "—"}
      />

      {/* CWD History */}
      {status.cwd_history.length > 0 && (
        <div className="card">
          <div className="card-label">Directory history</div>
          <ul className="cwd-list" style={{ marginTop: 6 }}>
            {status.cwd_history.map((path, i) => (
              <li className="cwd-item" key={i}>
                <span className="cwd-item-index">{i}</span>
                {truncate(path, 48)}
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

function ConfigPanel({
  config,
  autostartEnabled,
  onToggleAutostart,
}: {
  config: ConfigDto | null;
  autostartEnabled: boolean | null;
  onToggleAutostart: (v: boolean) => void;
}) {
  return (
    <div className="panel">
      <div className="panel-title">Configuration</div>

      {/* Autostart toggle */}
      <div className="toggle-row">
        <div>
          <div className="toggle-label">Start on login</div>
          <div className="toggle-desc">
            Launch automatically when Windows starts
          </div>
        </div>
        <label className="toggle">
          <input
            type="checkbox"
            checked={autostartEnabled ?? false}
            disabled={autostartEnabled === null}
            onChange={(e) => onToggleAutostart(e.target.checked)}
          />
          <span className="toggle-slider" />
        </label>
      </div>

      {config ? (
        <>
          <InfoCard
            label="Bot token"
            value={config.telegram_bot_token_masked}
          />
          <InfoCard
            label="Allowed user ID"
            value={
              config.telegram_allowed_user_id !== null
                ? String(config.telegram_allowed_user_id)
                : "(any)"
            }
          />
          <InfoCard label="Codex binary" value={config.codex_binary} />
          <InfoCard label="Default CWD" value={config.codex_cwd} />
          <InfoCard
            label="Model"
            value={config.codex_model ?? "(default)"}
          />
          <InfoCard
            label="Approval policy"
            value={config.codex_approval_policy}
          />
          <InfoCard
            label="Sandbox mode"
            value={config.codex_sandbox_mode ?? "(default)"}
          />
        </>
      ) : (
        [1, 2, 3].map((i) => (
          <div className="card" key={i} style={{ marginBottom: 12 }}>
            <div className="skeleton" style={{ width: "35%", marginBottom: 8 }} />
            <div className="skeleton" style={{ width: "60%" }} />
          </div>
        ))
      )}
    </div>
  );
}

// ── Root App ──────────────────────────────────────────────────────────────────

export default function App() {
  const [status, setStatus] = useState<StatusDto | null>(null);
  const [config, setConfig] = useState<ConfigDto | null>(null);
  const [statusError, setStatusError] = useState<string | null>(null);
  const [autostartEnabled, setAutostartEnabled] = useState<boolean | null>(null);

  // Determine header dot colour
  const dot = statusError
    ? "offline"
    : status?.active_turn_running
    ? "busy"
    : "online";

  // Fetch status (polling)
  const fetchStatus = useCallback(async () => {
    try {
      const s = await invoke<StatusDto>("get_status");
      setStatus(s);
      setStatusError(null);
    } catch (e: unknown) {
      setStatusError(String(e));
    }
  }, []);

  // Fetch config once
  useEffect(() => {
    invoke<ConfigDto>("get_config")
      .then(setConfig)
      .catch(() => {
        /* config unavailable – show loading skeleton */
      });
  }, []);

  // Poll status every 2 s
  useEffect(() => {
    fetchStatus();
    const id = setInterval(fetchStatus, 2000);
    return () => clearInterval(id);
  }, [fetchStatus]);

  // Fetch autostart state once
  useEffect(() => {
    isEnabled()
      .then(setAutostartEnabled)
      .catch(() => setAutostartEnabled(false));
  }, []);

  const handleToggleAutostart = useCallback(async (value: boolean) => {
    try {
      if (value) {
        await enable();
      } else {
        await disable();
      }
      setAutostartEnabled(value);
    } catch (e) {
      console.error("Failed to toggle autostart:", e);
    }
  }, []);

  return (
    <div className="layout">
      <header className="header">
        <div className={`header-dot ${dot}`} />
        <h1>Telegram Codex Bridge</h1>
        <span className="header-subtitle">
          {statusError
            ? "Bridge offline"
            : status?.active_turn_running
            ? "Turn in progress…"
            : "Listening"}
        </span>
      </header>

      <div className="content">
        <StatusPanel status={status} error={statusError} />
        <ConfigPanel
          config={config}
          autostartEnabled={autostartEnabled}
          onToggleAutostart={handleToggleAutostart}
        />
      </div>
    </div>
  );
}
