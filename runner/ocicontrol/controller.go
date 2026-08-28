package ocicontrol

import (
	"context"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"sync"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

type AgentRuntime interface {
	RecoverOCIRuntimeCapabilities(context.Context) error
	StopOCIRuntime(context.Context) error
	OCIRuntimeLive() bool
}

type ImageLoader interface {
	LoadImage(context.Context, string, io.Reader) (ocihelper.EnsureImageResponse, error)
}

type StopCycle interface {
	Stop(context.Context, func(context.Context) error) error
}

type SetupFunc func(context.Context, SetupRequest) (SetupResponse, error)

type ControllerConfig struct {
	IntentPath string
	Runtime    AgentRuntime
	Images     ImageLoader
	StopCycle  StopCycle
	Setup      SetupFunc
	Clock      Clock
	Doctor     func(context.Context) (DoctorResponse, error)
}

func (controller *Controller) Doctor(ctx context.Context) (DoctorResponse, error) {
	if controller.config.Doctor == nil {
		return DoctorResponse{}, runtimeUnavailable("OCI doctor is unavailable", nil)
	}
	return controller.config.Doctor(ctx)
}

// Controller is the only writer of durable OCI intent. One operation mutex
// spans compare-and-swap persistence and the associated local convergence so
// concurrent operator calls cannot overtake one another.
type Controller struct {
	config ControllerConfig
	mu     sync.Mutex
}

func NewController(config ControllerConfig) (*Controller, error) {
	if !filepath.IsAbs(config.IntentPath) {
		return nil, errors.New("OCI intent path must be absolute")
	}
	if config.Clock == nil {
		config.Clock = SystemClock{}
	}
	return &Controller{config: config}, nil
}

func (controller *Controller) Intent(ctx context.Context) (lima.OCIIntent, error) {
	return (lima.FileIntentSource{Path: controller.config.IntentPath}).ReadIntent(ctx)
}

func (controller *Controller) Setup(ctx context.Context, request SetupRequest) (SetupResponse, error) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if err := (lima.Sizing{Memory: request.VMMemory, CPUs: request.VMCPUs, Disk: request.VMDisk}).Validate(); err != nil {
		return SetupResponse{}, invalidRequest("invalid OCI setup sizing", err)
	}
	if controller.config.Setup == nil {
		return SetupResponse{ReasonCode: contract.CapabilityReasonPrerequisiteMissing, MissingCapability: "oci_setup_converger", Runbook: RunbookPath}, nil
	}
	response, err := controller.config.Setup(ctx, request)
	if err != nil {
		return response, err
	}
	if response.MissingCapability != "" || !response.Configured {
		return response, nil
	}
	if _, err := lima.InitializeOCIIntent(controller.config.IntentPath, controller.config.Clock.Now()); err != nil {
		return response, err
	}
	intent, err := controller.Intent(ctx)
	response.Intent = intent
	if !intent.Enabled {
		response.ReasonCode = contract.CapabilityReasonOCIIntentDisabled
	}
	return response, err
}

func (controller *Controller) Start(ctx context.Context, request IntentMutationRequest) (IntentResponse, error) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	current, readErr := controller.Intent(ctx)
	if readErr != nil {
		return IntentResponse{}, controlError(ErrorSetupRequired, http.StatusPreconditionFailed, "valid OCI intent is required", readErr)
	}
	intent, err := lima.SetOCIIntent(ctx, controller.config.IntentPath, request.ExpectedRevision, true, controller.config.Clock.Now())
	if err != nil {
		return IntentResponse{}, classifyIntentError(err)
	}
	if controller.config.Runtime == nil {
		return IntentResponse{Intent: intent}, runtimeUnavailable("OCI runtime is unavailable", nil)
	}
	if current.Enabled && controller.config.Runtime.OCIRuntimeLive() {
		return IntentResponse{Intent: intent, CapabilityPublished: true}, nil
	}
	if err := controller.config.Runtime.RecoverOCIRuntimeCapabilities(ctx); err != nil {
		return IntentResponse{Intent: intent}, runtimeUnavailable("OCI runtime recovery failed", err)
	}
	return IntentResponse{Intent: intent, CapabilityPublished: true}, nil
}

func (controller *Controller) Stop(ctx context.Context, request IntentMutationRequest) (IntentResponse, error) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	intent, err := lima.SetOCIIntent(ctx, controller.config.IntentPath, request.ExpectedRevision, false, controller.config.Clock.Now())
	if err != nil {
		return IntentResponse{}, classifyIntentError(err)
	}
	if controller.config.Runtime == nil {
		return IntentResponse{Intent: intent}, runtimeUnavailable("OCI runtime is unavailable", nil)
	}
	quiesce := controller.config.Runtime.StopOCIRuntime
	if controller.config.StopCycle != nil {
		err = controller.config.StopCycle.Stop(ctx, quiesce)
	} else {
		err = quiesce(ctx)
	}
	if err != nil {
		return IntentResponse{Intent: intent, RuntimeQuiesced: false}, controlError(ErrorRuntimeQuiescenceFailed, http.StatusConflict, "OCI runtime quiescence was not proven", err)
	}
	return IntentResponse{Intent: intent, RuntimeQuiesced: true}, nil
}

func (controller *Controller) LoadImage(ctx context.Context, archive io.Reader) (LoadImageResponse, error) {
	controller.mu.Lock()
	intent, err := controller.Intent(ctx)
	if err != nil {
		controller.mu.Unlock()
		return LoadImageResponse{}, err
	}
	if !intent.Enabled {
		controller.mu.Unlock()
		return LoadImageResponse{}, runtimeUnavailable("OCI intent is disabled", nil)
	}
	images := controller.config.Images
	controller.mu.Unlock()
	if images == nil {
		return LoadImageResponse{}, runtimeUnavailable("OCI image loading is unavailable", nil)
	}
	response, err := images.LoadImage(ctx, "", archive)
	if err != nil {
		return LoadImageResponse{}, err
	}
	return LoadImageResponse{
		TopLevelDigest: response.TopLevelDigest, PlatformDigest: response.PlatformDigest, Evidence: response.Evidence,
	}, nil
}

func classifyIntentError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var conflict *lima.IntentConflictError
	if errors.As(err, &conflict) {
		return controlError(ErrorIntentConflict, http.StatusConflict, "OCI intent revision conflict", err)
	}
	return controlError(ErrorSetupRequired, http.StatusPreconditionFailed, "valid OCI setup intent is required", err)
}
