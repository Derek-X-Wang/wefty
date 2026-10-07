package contract

import (
	"crypto/rand"
	"encoding/json"
	"net/http"
	"strings"
)

const RequestIDHeader = "X-Request-Id"
const L3RequestIDHeader = "X-L3-Request-Id"

// ObserveHTTPRequests generates a fresh ID even if the caller supplies a correlation
// header. It records no query, authorization value, or request body.
func ObserveHTTPRequests(layer string, logf func(string, ...any), next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := rand.Text()
		w.Header().Set(RequestIDHeader, id)
		if layer == "l3" {
			w.Header().Set(L3RequestIDHeader, id)
		}
		response := &responseWriter{ResponseWriter: w}
		defer func() {
			status := response.status
			if status == 0 {
				status = http.StatusOK
			}
			upstream := ""
			if echoed := w.Header().Get(RequestIDHeader); echoed != id {
				upstream = echoed
			}
			if logf != nil {
				logf("event=%s_request request_id=%s upstream_request_id=%s method=%s path=%q status=%d", layer, id, upstream, r.Method, r.URL.Path, status)
			}
		}()
		next.ServeHTTP(response, r)
	})
}

type responseWriter struct {
	http.ResponseWriter
	status    int
	rewritten bool
}

func (w *responseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *responseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	// net/http's mux emits plain-text 404/405 errors. Normalize those too,
	// without buffering successful result bodies or long-lived watches.
	if status >= 400 && strings.HasPrefix(w.Header().Get("Content-Type"), "text/plain") {
		code := ErrorInternal
		switch status {
		case http.StatusBadRequest, http.StatusMethodNotAllowed:
			code = ErrorInvalidRequest
		case http.StatusUnauthorized:
			code = ErrorUnauthorized
		case http.StatusForbidden:
			code = ErrorForbidden
		case http.StatusNotFound:
			code = ErrorNotFound
		}
		envelope := ErrorResponse{Error: AttachRequestID(w, APIError{Code: code, Message: http.StatusText(status), Retryable: false})}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Del("Content-Length")
		w.ResponseWriter.WriteHeader(status)
		_ = json.NewEncoder(w.ResponseWriter).Encode(envelope)
		w.rewritten = true
		return
	}
	w.ResponseWriter.WriteHeader(status)
}
func (w *responseWriter) Write(p []byte) (int, error) {
	if w.rewritten {
		return len(p), nil
	}
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(p)
}
func (w *responseWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

// AttachRequestID preserves an upstream error's correlation ID while adding L3's
// own. Copy details so a stored or shared error is never mutated by a request.
// Calls outside the HTTP middleware have no local ID and retain their input.
func AttachRequestID(w http.ResponseWriter, apiError APIError) APIError {
	local := w.Header().Get(L3RequestIDHeader)
	if local == "" {
		local = w.Header().Get(RequestIDHeader)
	}
	if local == "" {
		return apiError
	}
	if apiError.RequestID == "" {
		apiError.RequestID = local
	}
	w.Header().Set(RequestIDHeader, apiError.RequestID)
	if w.Header().Get(L3RequestIDHeader) != "" && apiError.RequestID != local {
		details := make(map[string]any, len(apiError.Details)+1)
		for key, value := range apiError.Details {
			details[key] = value
		}
		details["l3_request_id"] = local
		apiError.Details = details
	}
	return apiError
}
