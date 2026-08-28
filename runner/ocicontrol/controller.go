package ocicontrol

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

type AgentRuntime interface {
	RecoverOCIRuntimeCapabilities(context.Context) error
	StopOCIRuntime(context.Context) error
}

type ImageLoader interface {
	LoadImage(context.Context, string, io.Reader) (ocihelper.EnsureImageResponse, error)
}

type StopCycle interface {
	Stop(context.Context, func(context.Context) error) error
}

type SetupFunc func(context.Context, SetupRequest, lima.OCIIntent) (SetupResponse, error)

type ControllerConfig struct {
	IntentPath string
	Runtime    AgentRuntime
	Images     ImageLoader
	StopCycle  StopCycle
	Setup      SetupFunc
	Clock      Clock
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
		return SetupResponse{}, err
	}
	if _, err := lima.InitializeOCIIntent(controller.config.IntentPath, controller.config.Clock.Now()); err != nil {
		return SetupResponse{}, err
	}
	intent, err := controller.Intent(ctx)
	if err != nil {
		return SetupResponse{}, err
	}
	if controller.config.Setup == nil {
		return SetupResponse{
			Intent: intent, Convergence: ConvergenceUnchanged,
			ReasonCode:        contract.CapabilityReasonPrerequisiteMissing,
			MissingCapability: "oci_setup_converger", Runbook: RunbookPath,
		}, nil
	}
	response, err := controller.config.Setup(ctx, request, intent)
	if response.Intent.Version == 0 {
		response.Intent = intent
	}
	return response, err
}

func (controller *Controller) Start(ctx context.Context, request IntentMutationRequest) (IntentResponse, error) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	intent, err := lima.SetOCIIntent(controller.config.IntentPath, request.ExpectedRevision, true, controller.config.Clock.Now())
	if err != nil {
		return IntentResponse{}, err
	}
	if controller.config.Runtime == nil {
		return IntentResponse{Intent: intent}, errors.New("OCI runtime is unavailable")
	}
	if err := controller.config.Runtime.RecoverOCIRuntimeCapabilities(ctx); err != nil {
		return IntentResponse{Intent: intent}, err
	}
	return IntentResponse{Intent: intent, CapabilityPublished: true}, nil
}

func (controller *Controller) Stop(ctx context.Context, request IntentMutationRequest) (IntentResponse, error) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	intent, err := lima.SetOCIIntent(controller.config.IntentPath, request.ExpectedRevision, false, controller.config.Clock.Now())
	if err != nil {
		return IntentResponse{}, err
	}
	if controller.config.Runtime == nil {
		return IntentResponse{Intent: intent}, errors.New("OCI runtime is unavailable")
	}
	quiesce := controller.config.Runtime.StopOCIRuntime
	if controller.config.StopCycle != nil {
		err = controller.config.StopCycle.Stop(ctx, quiesce)
	} else {
		err = quiesce(ctx)
	}
	if err != nil {
		return IntentResponse{Intent: intent}, err
	}
	return IntentResponse{Intent: intent, RuntimeQuiesced: true}, nil
}

func (controller *Controller) LoadImage(ctx context.Context, archive io.Reader) (LoadImageResponse, error) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	intent, err := controller.Intent(ctx)
	if err != nil {
		return LoadImageResponse{}, err
	}
	if !intent.Enabled {
		return LoadImageResponse{}, errors.New("OCI intent is disabled")
	}
	if controller.config.Images == nil {
		return LoadImageResponse{}, errors.New("OCI image loading is unavailable")
	}
	response, err := controller.config.Images.LoadImage(ctx, "", archive)
	if err != nil {
		return LoadImageResponse{}, err
	}
	return LoadImageResponse{
		TopLevelDigest: response.TopLevelDigest, PlatformDigest: response.PlatformDigest, Evidence: response.Evidence,
	}, nil
}
