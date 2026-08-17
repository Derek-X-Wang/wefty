package process

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const (
	guardianArg        = "__wefty_guardian"
	guardianEndpointFD = 3
)

type guardianMessageType string

const (
	guardianMessageStart     guardianMessageType = "start"
	guardianMessageStop      guardianMessageType = "stop"
	guardianMessageStarted   guardianMessageType = "started"
	guardianMessageReadiness guardianMessageType = "readiness"
	guardianMessageExited    guardianMessageType = "exited"
)

// guardianControlMessage is deliberately small and structured: it is the
// test seam for the private agent/guardian process boundary. Payload details
// travel over the inherited endpoint, never through argv.
type guardianControlMessage struct {
	Type  guardianMessageType `json:"type"`
	Start *guardianStart      `json:"start,omitempty"`
}

type guardianStart struct {
	Path                     string        `json:"path"`
	Args                     []string      `json:"args"`
	Directory                string        `json:"directory"`
	Environment              []string      `json:"environment"`
	TerminationGrace         time.Duration `json:"termination_grace"`
	ServiceAddress           string        `json:"service_address,omitempty"`
	StartupReadinessDeadline time.Duration `json:"startup_readiness_deadline,omitempty"`
	ReadinessProbeInterval   time.Duration `json:"readiness_probe_interval,omitempty"`
	ReadinessConnectTimeout  time.Duration `json:"readiness_connect_timeout,omitempty"`
}

type guardianStatusMessage struct {
	Type             guardianMessageType     `json:"type"`
	PID              int                     `json:"pid,omitempty"`
	ProcessGroupID   int                     `json:"process_group_id,omitempty"`
	Result           *contract.ProcessResult `json:"result,omitempty"`
	StartupSatisfied bool                    `json:"startup_satisfied,omitempty"`
	Ready            *bool                   `json:"ready,omitempty"`
}

// IsGuardianInvocation recognizes the private mode before normal flag parsing.
func IsGuardianInvocation(arguments []string) bool {
	return len(arguments) > 1 && arguments[1] == guardianArg
}

// RunGuardianInvocation serves the inherited control endpoint for the private
// wefty-agent mode. The endpoint is the only capability passed by the agent.
func RunGuardianInvocation(arguments []string) error {
	if len(arguments) != 2 || arguments[1] != guardianArg {
		return errors.New("invalid guardian invocation")
	}
	endpoint := os.NewFile(guardianEndpointFD, "wefty-guardian-control")
	if endpoint == nil {
		return errors.New("guardian control endpoint is unavailable")
	}
	defer endpoint.Close()
	return serveGuardian(endpoint, os.Stdout, os.Stderr)
}

func decodeGuardianStatus(decoder *json.Decoder) (guardianStatusMessage, error) {
	var status guardianStatusMessage
	if err := decoder.Decode(&status); err != nil {
		return guardianStatusMessage{}, err
	}
	switch status.Type {
	case guardianMessageStarted:
		if status.PID <= 0 || status.ProcessGroupID <= 0 || status.Result != nil || status.Ready != nil {
			return guardianStatusMessage{}, fmt.Errorf("invalid guardian started message")
		}
	case guardianMessageReadiness:
		if status.PID != 0 || status.ProcessGroupID != 0 || status.Result != nil || !status.StartupSatisfied || status.Ready == nil {
			return guardianStatusMessage{}, fmt.Errorf("invalid guardian readiness message")
		}
	case guardianMessageExited:
		if status.PID != 0 || status.ProcessGroupID != 0 || status.Result == nil || status.Ready != nil {
			return guardianStatusMessage{}, fmt.Errorf("invalid guardian exited message")
		}
	default:
		return guardianStatusMessage{}, fmt.Errorf("unknown guardian status type %q", status.Type)
	}
	return status, nil
}
