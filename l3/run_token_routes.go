package l3

import "net/http"

// RunTokenRoute is one L3 route that serves a request authenticated by a run
// token. The agent's run-surface workflow bridge mirrors this list exactly and
// refuses every other /l3 path: the bridge dials L3 as the agent's own Fabric
// identity, so any route L3 authorizes by node identity alone would otherwise
// be reachable by the workload (#595). L3 remains authoritative for scope.
//
// Routes a run token is refused on are deliberately absent: the general and
// origin-scoped Run listings, rerun, the reserved cancel, Workflow
// administration, and the Computer-pass and node-administration routes.
type RunTokenRoute struct {
	Method string
	Path   string
}

var runTokenRoutes = []RunTokenRoute{
	{Method: http.MethodPost, Path: "/v1/runs"},
	{Method: http.MethodGet, Path: "/v1/runs/{run_id}"},
	{Method: http.MethodGet, Path: "/v1/runs/{run_id}/lineage"},
	{Method: http.MethodGet, Path: "/v1/runs/{run_id}/logs"},
	{Method: http.MethodGet, Path: "/v1/runs/{run_id}/execution"},
	{Method: http.MethodGet, Path: "/v1/runs/{run_id}/result"},
	{Method: http.MethodPost, Path: "/v1/runs/{run_id}/envelopes"},
	{Method: http.MethodPost, Path: "/v1/runs/{run_id}/gates"},
}

func RunTokenRoutes() []RunTokenRoute {
	return append([]RunTokenRoute(nil), runTokenRoutes...)
}
