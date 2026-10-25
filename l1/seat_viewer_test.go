package l1

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestSeatViewerTargetAdminRead(t *testing.T) {
	f, id, secret, input := liveSeatForCopy(t)
	identity := fabric.Identity{FabricID: "test-fabric", UserID: "owner", DeviceID: "owner-device"}
	challenge, err := f.store.InitiateAdminBootstrap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.BootstrapAdmin(t.Context(), identity, challenge.Nonce); err != nil {
		t.Fatal(err)
	}
	ready := true
	endpoint := "ws://seat-node:80/websockify"
	if _, err = f.store.SetAttemptPublication(t.Context(), "fabric-computer-node", input.JobID, input.AttemptID, PublicationRequest{FencingToken: input.FencingToken, Ready: &ready, DisplayEndpoint: &endpoint}); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []bool{true, false} {
		caller := identity
		if !owner {
			caller.UserID = "visitor"
		}
		server, err := NewServer(viewerIdentityFabric{Identity: caller}, f.store, ServerConfig{AllowSelfAssertedPersonIdentities: true})
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodGet, "/v1/seats/"+id+"/viewer-target", nil)
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if !owner {
			if response.Code != 403 || !strings.Contains(response.Body.String(), "admin_required") {
				t.Fatalf("visitor=%d %s", response.Code, response.Body)
			}
			continue
		}
		var target struct {
			URL string `json:"url"`
		}
		if json.Unmarshal(response.Body.Bytes(), &target) != nil || response.Code != 200 || target.URL != "http://seat-node:80/v1/seats/"+id+"/person/viewer" {
			t.Fatalf("owner=%d %s", response.Code, response.Body)
		}
		if strings.Contains(response.Body.String(), secret) {
			t.Fatal("discovery leaked Seat credential")
		}
		request.Header.Set(SeatCredentialHeader, secret)
		response = httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code == 200 {
			t.Fatal("Seat credential reached person discovery")
		}
	}
	// Admin discovery must be a query-only snapshot. It must not refresh Seat life.
	before, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(30 * time.Minute)
	server, err := NewServer(viewerIdentityFabric{Identity: identity}, f.store, ServerConfig{AllowSelfAssertedPersonIdentities: true})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/seats/"+id+"/viewer-target", nil))
	after, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || before.State != after.State {
		t.Fatalf("discovery changed Seat: %+v %v", after, err)
	}
	if err = f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	after, err = f.store.GetSeatRequest(t.Context(), id)
	if err != nil || after.State != SeatEnding {
		t.Fatalf("discovery renewed Seat life: %+v %v", after, err)
	}
}

type viewerIdentityFabric struct {
	fabric.Fabric
	Identity fabric.Identity
}

func (f viewerIdentityFabric) WhoIs(_ context.Context, _ string) (fabric.Identity, error) {
	return f.Identity, nil
}

func (viewerIdentityFabric) PersonIdentityTrust() fabric.PersonIdentityTrust {
	return fabric.PersonIdentitySelfAsserted
}
