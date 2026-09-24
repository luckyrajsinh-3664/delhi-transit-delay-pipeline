const BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

async function get(path) {
  const res = await fetch(`${BASE_URL}${path}`);
  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = await res.json();
      detail = body.detail || detail;
    } catch {
      /* response wasn't JSON */
    }
    throw new Error(detail);
  }
  return res.json();
}

export const api = {
  health: () => get("/health"),
  slowestRoutes: (limit = 10, minMoving = 3) =>
    get(`/routes/slowest?limit=${limit}&min_moving=${minMoving}`),
  routeSpeed: (routeId, minMoving = 3) =>
    get(`/routes/${encodeURIComponent(routeId)}/speed?min_moving=${minMoving}`),
  predict: (routeId, hour, dayOfWeek) =>
    get(
      `/predict?route_id=${encodeURIComponent(routeId)}&hour=${hour}&day_of_week=${dayOfWeek}`
    ),
};