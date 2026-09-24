import HealthBadge from "./components/HealthBadge";
import SlowestRoutes from "./components/SlowestRoutes";
import RouteLookup from "./components/RouteLookup";
import PredictForm from "./components/PredictForm";
import "./App.css";

export default function App() {
  return (
    <div className="shell">
      <header className="masthead">
        <div className="masthead-id">
          <div className="badge-mark" aria-hidden="true">RR</div>
          <div>
            <h1>RouteRadar</h1>
            <p>Delhi bus congestion, tracked from live GPS and refreshed by the pipeline every hour.</p>
          </div>
        </div>
        <HealthBadge />
      </header>

      <SlowestRoutes />

      <div className="split">
        <RouteLookup />
        <PredictForm />
      </div>
    </div>
  );
}