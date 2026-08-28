package l3

import "net/http"

// ComputerTokenRoute is one L3 route a verified Computer pass may reach.
// The agent bridge mirrors this registry only as transport defense in depth;
// L3 remains authoritative for every request.
type ComputerTokenRoute struct {
	Method string
	Path   string
}

var computerTokenRoutes = []ComputerTokenRoute{
	{Method: http.MethodGet, Path: "/v1/computer/self"},
	{Method: http.MethodGet, Path: "/v1/runs"},
	{Method: http.MethodPost, Path: "/v1/runs"},
	{Method: http.MethodGet, Path: "/v1/runs/{run_id}"},
	{Method: http.MethodGet, Path: "/v1/runs/{run_id}/lineage"},
	{Method: http.MethodGet, Path: "/v1/runs/{run_id}/logs"},
}

func ComputerTokenRoutes() []ComputerTokenRoute {
	return append([]ComputerTokenRoute(nil), computerTokenRoutes...)
}

// registerComputerTokenRoutes makes the exported registry load-bearing for
// production routing. Adding or removing a Computer-token handler therefore
// changes the same list the transport mirror must match.
func (s *Server) registerComputerTokenRoutes(runs *http.ServeMux, root *http.ServeMux) {
	for _, route := range computerTokenRoutes {
		var handler http.HandlerFunc
		switch route.Path {
		case "/v1/computer/self":
			handler = s.getComputerSelf
		case "/v1/runs":
			if route.Method == http.MethodGet {
				handler = s.listComputerRuns
			} else {
				handler = s.createRun
			}
		case "/v1/runs/{run_id}":
			handler = s.getRun
		case "/v1/runs/{run_id}/lineage":
			handler = s.getRunLineage
		case "/v1/runs/{run_id}/logs":
			handler = s.getRunLogs
		default:
			panic("unhandled Computer token route: " + route.Path)
		}
		pattern := route.Method + " " + route.Path
		if route.Path == "/v1/computer/self" {
			root.Handle(pattern, s.authenticateFabric(s.authorize(handler)))
		} else {
			runs.HandleFunc(pattern, handler)
		}
	}
}
