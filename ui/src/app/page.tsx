"use client";

import { useState, useEffect } from "react";
import { fetchStatus, triggerLoad, triggerCrash } from "./actions";

export default function Home() {
  const [statusData, setStatusData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [toast, setToast] = useState<{ message: string; type: "success" | "error" } | null>(null);

  useEffect(() => {
    async function loadData() {
      const data = await fetchStatus();
      setStatusData(data);
      setLoading(false);
    }
    loadData();
    // Poll every 5 seconds
    const interval = setInterval(loadData, 5000);
    return () => clearInterval(interval);
  }, []);

  const showToast = (message: string, type: "success" | "error") => {
    setToast({ message, type });
    setTimeout(() => setToast(null), 5000);
  };

  const handleTriggerLoad = async () => {
    const res = await triggerLoad(60);
    if (res.success) {
      showToast("CPU Load Triggered (60s). Watch HPA scale up!", "success");
    } else {
      showToast("Failed to trigger load", "error");
    }
  };

  const handleTriggerCrash = async () => {
    const res = await triggerCrash();
    if (res.success) {
      showToast("Pod Crash Triggered. It will restart shortly.", "success");
    } else {
      showToast("Failed to trigger crash", "error");
    }
  };

  if (loading) {
    return (
      <main>
        <div className="header">
          <h1>Self-Healing EKS</h1>
          <p>Connecting to cluster...</p>
        </div>
      </main>
    );
  }

  const { success, root, health, version, db } = statusData;

  return (
    <main>
      <div className="header">
        <h1>Self-Healing EKS <span className={success ? "pulse" : ""} style={{ marginLeft: "10px" }}></span></h1>
        <p>Predictive Operations & Chaos Engineering Dashboard</p>
      </div>

      <div className="grid">
        <div className="panel">
          <div className="panel-title">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M22 12h-4l-3 9L9 3l-3 9H2" />
            </svg>
            System Health
          </div>
          <div className="data-row">
            <span className="data-label">API Status</span>
            <span className={health?.status === "ok" ? "status-ok" : "status-error"}>
              {health?.status || "UNREACHABLE"}
            </span>
          </div>
          <div className="data-row">
            <span className="data-label">DB Connection</span>
            <span className={db?.status === "connected" ? "status-ok" : "status-error"}>
              {db?.status || "FAILED"}
            </span>
          </div>
          <div className="data-row">
            <span className="data-label">Postgres Version</span>
            <span className="data-value">{db?.server?.split(" ")[1] || "-"}</span>
          </div>
        </div>

        <div className="panel">
          <div className="panel-title">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <rect x="2" y="3" width="20" height="14" rx="2" ry="2" />
              <line x1="8" y1="21" x2="16" y2="21" />
              <line x1="12" y1="17" x2="12" y2="21" />
            </svg>
            Environment
          </div>
          <div className="data-row">
            <span className="data-label">Active Pod</span>
            <span className="data-value">{root?.pod || "-"}</span>
          </div>
          <div className="data-row">
            <span className="data-label">Node</span>
            <span className="data-value">{root?.node || "-"}</span>
          </div>
          <div className="data-row">
            <span className="data-label">App Version</span>
            <span className="data-value">{version?.version || "-"}</span>
          </div>
          <div className="data-row">
            <span className="data-label">Build Time</span>
            <span className="data-value">{version?.build_time || "-"}</span>
          </div>
        </div>

        <div className="panel" style={{ gridColumn: "1 / -1" }}>
          <div className="panel-title" style={{ borderColor: "rgba(239, 68, 68, 0.3)" }}>
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="var(--accent-red)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" />
            </svg>
            Chaos Controls
          </div>
          <p style={{ color: "var(--text-muted)", marginBottom: "1.5rem" }}>
            Trigger these events to observe the platform's self-healing and predictive capabilities in real-time via Grafana and the Kubernetes dashboard.
          </p>
          <div className="button-group" style={{ flexDirection: "row", gap: "1.5rem" }}>
            <button className="btn btn-primary" onClick={handleTriggerLoad} style={{ flex: 1 }}>
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12" /></svg>
              Simulate High CPU Load (60s)
            </button>
            <button className="btn btn-danger" onClick={handleTriggerCrash} style={{ flex: 1 }}>
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10" /><line x1="15" y1="9" x2="9" y2="15" /><line x1="9" y1="9" x2="15" y2="15" /></svg>
              Trigger Pod Crash
            </button>
          </div>
        </div>
      </div>

      {toast && (
        <div className="toast" style={{ borderLeft: `4px solid ${toast.type === "success" ? "var(--accent-green)" : "var(--accent-red)"}` }}>
          <span>{toast.message}</span>
        </div>
      )}
    </main>
  );
}
