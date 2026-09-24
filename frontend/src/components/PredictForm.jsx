import { useState } from "react";
import { api } from "../api";

const DAYS = [
  { value: 1, label: "Sunday" },
  { value: 2, label: "Monday" },
  { value: 3, label: "Tuesday" },
  { value: 4, label: "Wednesday" },
  { value: 5, label: "Thursday" },
  { value: 6, label: "Friday" },
  { value: 7, label: "Saturday" },
];

export default function PredictForm() {
  const [routeId, setRouteId] = useState("");
  const [hour, setHour] = useState(9);
  const [dayOfWeek, setDayOfWeek] = useState(2);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  const submit = async (e) => {
    e.preventDefault();
    if (!routeId.trim()) return;
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      setResult(await api.predict(routeId.trim(), hour, dayOfWeek));
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <section className="panel">
      <div className="panel-head">
        <h2>Predict a speed</h2>
      </div>
      <p className="panel-sub">
        Asks the trained model for an expected speed, given a route, hour and day.
      </p>

      <form onSubmit={submit}>
        <div className="field-row">
          <input
            type="text"
            inputMode="numeric"
            placeholder="Route number"
            value={routeId}
            onChange={(e) => setRouteId(e.target.value)}
          />
        </div>
        <div className="field-row" style={{ marginTop: 10 }}>
          <label className="field-stack">
            Hour
            <select value={hour} onChange={(e) => setHour(Number(e.target.value))}>
              {Array.from({ length: 24 }, (_, h) => (
                <option key={h} value={h}>
                  {String(h).padStart(2, "0")}:00
                </option>
              ))}
            </select>
          </label>
          <label className="field-stack">
            Day
            <select
              value={dayOfWeek}
              onChange={(e) => setDayOfWeek(Number(e.target.value))}
            >
              {DAYS.map((d) => (
                <option key={d.value} value={d.value}>
                  {d.label}
                </option>
              ))}
            </select>
          </label>
          <button className="btn" type="submit" disabled={loading} style={{ alignSelf: "end" }}>
            {loading ? "Predicting…" : "Predict"}
          </button>
        </div>
      </form>

      {error && <p className="msg msg-error">{error}</p>}

      {result && (
        <div className="readout">
          <span className="readout-value">{result.predicted_speed_kmph}</span>
          <span className="readout-unit">km/h</span>
          <p className="readout-detail">
            Route {result.route_id}, {String(result.hour_key).padStart(2, "0")}:00,{" "}
            {DAYS.find((d) => d.value === result.day_of_week)?.label}
            {result.route_historical_avg_kmph != null &&
              ` — historical average ${result.route_historical_avg_kmph} km/h`}
          </p>
          <p className="readout-note">{result.note}</p>
        </div>
      )}
    </section>
  );
}