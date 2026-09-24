import { useEffect, useState } from "react";
import { api } from "../api";

export default function SlowestRoutes() {
  const [rows, setRows] = useState(null);
  const [error, setError] = useState(null);
  const [limit, setLimit] = useState(10);

  useEffect(() => {
    setRows(null);
    setError(null);
    api
      .slowestRoutes(limit)
      .then(setRows)
      .catch((e) => setError(e.message));
  }, [limit]);

  const maxSpeed = rows?.length
    ? Math.max(...rows.map((r) => r.avg_moving_speed_kmph ?? 0))
    : 1;

  return (
    <section className="panel">
      <div className="panel-head">
        <h2>Where buses are stuck right now</h2>
        <label className="field-inline">
          Show
          <select value={limit} onChange={(e) => setLimit(Number(e.target.value))}>
            <option value={5}>5</option>
            <option value={10}>10</option>
            <option value={15}>15</option>
            <option value={25}>25</option>
          </select>
        </label>
      </div>
      <p className="panel-sub">
        Route-hours ranked by average moving speed, slowest first.
      </p>

      {error && (
        <p className="msg msg-error">
          Couldn't load this — {error}
        </p>
      )}
      {!rows && !error && <p className="msg msg-muted">Loading route data…</p>}
      {rows && rows.length === 0 && (
        <p className="msg msg-muted">No route-hours with enough moving buses yet.</p>
      )}

      {rows && rows.length > 0 && (
        <div className="board">
          {rows.map((r, i) => {
            const speed = r.avg_moving_speed_kmph ?? 0;
            const pct = maxSpeed ? Math.max(6, (speed / maxSpeed) * 100) : 0;
            return (
              <div className="board-row" key={i}>
                <div className="board-route">{r.route_id}</div>
                <div>
                  <div className="board-meta">
                    {r.full_date} · {r.hour_label} · {r.n_moving} buses moving
                  </div>
                  <div className="board-bar-track">
                    <div className="board-bar-fill" style={{ width: `${pct}%` }} />
                  </div>
                </div>
                <div className="board-speed-col">
                  <div className="board-speed">{speed.toFixed(2)} km/h</div>
                  {r.n_bunched > 0 && (
                    <div className="board-bunch">{r.n_bunched} bunched</div>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      )}
    </section>
  );
}