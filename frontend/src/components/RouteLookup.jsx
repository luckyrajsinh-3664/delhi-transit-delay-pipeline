import { useState } from "react";
import { api } from "../api";

export default function RouteLookup() {
  const [routeId, setRouteId] = useState("");
  const [rows, setRows] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  const search = async (e) => {
    e.preventDefault();
    if (!routeId.trim()) return;
    setLoading(true);
    setError(null);
    setRows(null);
    try {
      setRows(await api.routeSpeed(routeId.trim()));
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <section className="panel">
      <div className="panel-head">
        <h2>Look up a route</h2>
      </div>
      <p className="panel-sub">Every hour a route has been observed with data behind it.</p>

      <form className="field-row" onSubmit={search}>
        <input
          type="text"
          inputMode="numeric"
          placeholder="Route number, e.g. 1914"
          value={routeId}
          onChange={(e) => setRouteId(e.target.value)}
        />
        <button className="btn" type="submit" disabled={loading}>
          {loading ? "Searching…" : "Search"}
        </button>
      </form>

      {error && <p className="msg msg-error">{error}</p>}

      {rows && (
        <table className="lookup-table">
          <thead>
            <tr>
              <th>Date</th>
              <th>Hour</th>
              <th>Moving</th>
              <th>Speed</th>
              <th>Bunched</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i}>
                <td>{r.full_date}</td>
                <td>{r.hour_label}</td>
                <td className="num">{r.n_moving}</td>
                <td className="num">{r.avg_moving_speed_kmph ?? "—"}</td>
                <td className="num">{r.n_bunched}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}