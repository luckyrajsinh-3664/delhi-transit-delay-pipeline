import { useEffect, useState } from "react";
import { api } from "../api";

export default function HealthBadge() {
  const [health, setHealth] = useState(null);
  const [unreachable, setUnreachable] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const check = () =>
      api
        .health()
        .then((h) => !cancelled && (setHealth(h), setUnreachable(false)))
        .catch(() => !cancelled && setUnreachable(true));
    check();
    const id = setInterval(check, 15000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  if (unreachable) {
    return (
      <div className="status">
        <span className="status-dot is-down" aria-hidden="true" />
        API not reachable — start it with uvicorn
      </div>
    );
  }
  if (!health) {
    return (
      <div className="status">
        <span className="status-dot" aria-hidden="true" />
        Checking API…
      </div>
    );
  }

  const state = health.status === "ok" ? "is-ok" : "is-degraded";
  const label =
    health.status === "ok"
      ? health.model_loaded
        ? "Warehouse and model connected"
        : "Warehouse connected, model not loaded"
      : "Degraded — database unreachable";

  return (
    <div className="status">
      <span className={`status-dot ${state}`} aria-hidden="true" />
      {label}
    </div>
  );
}