package ocihelper

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"
)

const (
	MethodApplyComputerSeats     Method = "ApplyComputerSeats"
	MethodObserveComputerSeats   Method = "ObserveComputerSeats"
	ComputerSeatReadinessTimeout        = 60 * time.Second
)

// SeatOperationRefusalReason is a typed request/runtime refusal, distinct
// from a failure to read or publish runtime evidence.
type SeatOperationRefusalReason string

const (
	SeatRefusalInPlaceChange       SeatOperationRefusalReason = "in_place_change_requires_remove"
	SeatRefusalCleanupNotConfirmed SeatOperationRefusalReason = "cleanup_not_confirmed"
	SeatRefusalGenerationMismatch  SeatOperationRefusalReason = "generation_mismatch"
	SeatRefusalAttemptNotLive      SeatOperationRefusalReason = "attempt_not_live"
	SeatRefusalTmpfsFloor          SeatOperationRefusalReason = "tmpfs_floor"
	SeatRefusalMemoryCapacity      SeatOperationRefusalReason = "memory_capacity"
	SeatRefusalMemoryGrowFailed    SeatOperationRefusalReason = "memory_grow_failed"
	SeatRefusalMemoryBusy          SeatOperationRefusalReason = "memory_busy"
	SeatRefusalMemoryThawFailed    SeatOperationRefusalReason = "memory_thaw_failed"
)

type SeatOperationRefusalError struct{ Reason SeatOperationRefusalReason }

func (e *SeatOperationRefusalError) Error() string {
	return "Seat operation refused: " + string(e.Reason)
}

// ComputerSeat is the image's place configuration, not a public Seat identity.
type ComputerSeat struct {
	Place       int    `json:"place"`
	User        string `json:"user"`
	ViewPort    uint16 `json:"view_port"`
	ControlPort uint16 `json:"control_port"`
}
type ApplyComputerSeatsRequest struct {
	Authority AttemptAuthority `json:"authority"`
	Seats     []ComputerSeat   `json:"seats"`
}
type ApplyComputerSeatsResponse struct {
	Generation uint64 `json:"generation"`
}
type ObserveComputerSeatsRequest struct {
	Authority  AttemptAuthority `json:"authority"`
	Generation uint64           `json:"generation"`
}
type ComputerSeatObservation struct {
	Place           int               `json:"place"`
	State           string            `json:"state"` // starting, ready, failed, removing, cleaned
	Reason          string            `json:"reason"`
	CleanupComplete bool              `json:"cleanup_complete"`
	Endpoints       map[string]string `json:"endpoints,omitempty"`
}
type ObserveComputerSeatsResponse struct {
	Generation uint64                    `json:"generation"`
	Seats      []ComputerSeatObservation `json:"seats"`
}
type ComputerSeatsEngine interface {
	ApplyComputerSeats(context.Context, ApplyComputerSeatsRequest) (ApplyComputerSeatsResponse, error)
	ObserveComputerSeats(context.Context, ObserveComputerSeatsRequest) (ObserveComputerSeatsResponse, error)
}

type computerSeatImageConfig struct {
	ComputerSeat
	ViewSocket    string `json:"view_socket"`
	ControlSocket string `json:"control_socket"`
}

type computerSeatAnswer struct {
	Version         int    `json:"version"`
	Generation      uint64 `json:"generation"`
	Place           int    `json:"place"`
	State           string `json:"state"`
	Reason          string `json:"reason"`
	CleanupComplete bool   `json:"cleanup_complete"`
}
type computerSeatEntry struct {
	incarnation      uint64
	endpointPins     map[string]*os.File
	endpointRevision uint64
	endpointScope    string
	config           ComputerSeat
	deadline         time.Time
	wanted           bool
	readyGeneration  uint64
	ready            bool
	failure          string
	cleaned          bool
	memoryReturned   bool
}

// Serialized by the attempt's seatsMu. State belongs to one exact attempt;
// losing it never creates cleanup evidence. Durable reconciliation is #1142.
type computerSeats struct {
	clock             Clock
	directory         string
	generation        uint64
	endpointScope     string
	entries           map[int]*computerSeatEntry
	wanted            []ComputerSeat
	pendingShrinkFrom int64 // old memory limit until an uncertain shrink is read back
}

func newComputerSeats(clock Clock, directory string) *computerSeats {
	if clock == nil {
		clock = systemClock{}
	}
	return &computerSeats{endpointScope: rand.Text(), clock: clock, directory: directory, entries: map[int]*computerSeatEntry{}}
}
func validateComputerSeats(seats []ComputerSeat) error {
	if len(seats) > 4 {
		return errors.New("at most four Seat places")
	}
	places, ports := map[int]bool{}, map[uint16]bool{}
	for _, s := range seats {
		if s.Place < 1 || s.Place > 4 || s.User != fmt.Sprintf("seat%d", s.Place) || places[s.Place] || s.ViewPort < 1024 || s.ControlPort < 1024 || s.ViewPort == s.ControlPort || ports[s.ViewPort] || ports[s.ControlPort] {
			return errors.New("invalid Seat place, user or duplicate endpoint")
		}
		places[s.Place], ports[s.ViewPort], ports[s.ControlPort] = true, true, true
	}
	return nil
}
func (s *computerSeats) apply(wanted []ComputerSeat) (ApplyComputerSeatsResponse, error) {
	if err := validateComputerSeats(wanted); err != nil {
		return ApplyComputerSeatsResponse{}, err
	}
	wanted = slices.Clone(wanted)
	slices.SortFunc(wanted, func(a, b ComputerSeat) int { return a.Place - b.Place })
	if s.generation > 0 && slices.Equal(s.wanted, wanted) {
		return ApplyComputerSeatsResponse{s.generation}, nil
	}
	for _, config := range wanted {
		if entry := s.entries[config.Place]; entry != nil {
			if entry.wanted && entry.config != config {
				return ApplyComputerSeatsResponse{}, &SeatOperationRefusalError{Reason: SeatRefusalInPlaceChange}
			}
			if !entry.wanted && !entry.cleaned {
				return ApplyComputerSeatsResponse{}, &SeatOperationRefusalError{Reason: SeatRefusalCleanupNotConfirmed}
			}
		}
		for _, entry := range s.entries {
			if !entry.cleaned && entry.config.Place != config.Place && (entry.config.ViewPort == config.ViewPort || entry.config.ControlPort == config.ViewPort || entry.config.ViewPort == config.ControlPort || entry.config.ControlPort == config.ControlPort) {
				return ApplyComputerSeatsResponse{}, &SeatOperationRefusalError{Reason: SeatRefusalCleanupNotConfirmed}
			}
		}
	}
	generation := s.generation + 1
	if generation == 0 {
		return ApplyComputerSeatsResponse{}, errors.New("Seat generation exhausted")
	}
	directive := struct {
		Version    int                       `json:"version"`
		Generation uint64                    `json:"generation"`
		Seats      []computerSeatImageConfig `json:"seats"`
	}{Version: 1, Generation: generation, Seats: []computerSeatImageConfig{}}
	for _, config := range wanted {
		directive.Seats = append(directive.Seats, computerSeatImageConfig{config, seatEndpointPath(config.Place, "view"), seatEndpointPath(config.Place, "control")})
	}
	payload, err := json.Marshal(directive)
	if err != nil {
		return ApplyComputerSeatsResponse{}, err
	}
	// The budget starts before publication. A slow fsync cannot extend it.
	started := s.clock.Now()
	if err := publishComputerSeatDirective(s.directory, payload); err != nil {
		return ApplyComputerSeatsResponse{}, err
	}
	wantedPlaces := map[int]bool{}
	for _, config := range wanted {
		wantedPlaces[config.Place] = true
	}
	for place, entry := range s.entries {
		if !wantedPlaces[place] {
			entry.wanted = false
			entry.closeEndpointPins()
		}
	}
	for _, config := range wanted {
		entry := s.entries[config.Place]
		if entry == nil || !entry.wanted {
			entry = &computerSeatEntry{endpointScope: s.endpointScope, incarnation: generation, config: config, deadline: started.Add(ComputerSeatReadinessTimeout)}
			s.entries[config.Place] = entry
		}
		entry.wanted = true
	}
	s.generation, s.wanted = generation, wanted
	return ApplyComputerSeatsResponse{generation}, nil
}
func publishComputerSeatDirective(directory string, payload []byte) (err error) {
	f, err := os.CreateTemp(directory, ".seats-*.json")
	if err != nil {
		return err
	}
	defer func() { f.Close(); os.Remove(f.Name()) }()
	if err = f.Chmod(0444); err != nil {
		return err
	}
	if _, err = f.Write(payload); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), filepath.Join(directory, "seats.json"))
}
func decodeSeatDocument(payload []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(payload))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("trailing Seat answer data")
	}
	return nil
}
func (s *computerSeats) observe(ctx context.Context, generation uint64, read func(string) ([]byte, error), probe func(context.Context, uint16) error, cleanup func(ComputerSeat) error) (ObserveComputerSeatsResponse, error) {
	if generation == 0 || generation != s.generation {
		return ObserveComputerSeatsResponse{}, &SeatOperationRefusalError{Reason: SeatRefusalGenerationMismatch}
	}
	result := ObserveComputerSeatsResponse{Generation: generation, Seats: []ComputerSeatObservation{}}
	places := make([]int, 0, len(s.entries))
	for place := range s.entries {
		places = append(places, place)
	}
	slices.Sort(places)
	for _, place := range places {
		entry := s.entries[place]
		observation := ComputerSeatObservation{Place: place, State: "starting"}
		if !entry.wanted {
			// Cleanup is final for this incarnation. Its ports may already
			// belong to a different Seat; a later directive cannot revoke it.
			if entry.cleaned {
				observation.State, observation.CleanupComplete = "cleaned", true
				result.Seats = append(result.Seats, observation)
				continue
			}
			observation.State = "removing"
			payload, readErr := read(fmt.Sprintf("%d.json", place))
			var failed computerSeatAnswer
			if readErr == nil && decodeSeatDocument(payload, &failed) == nil && failed.Version == 1 && failed.Generation == generation && failed.Place == place && failed.State == "failed" && failed.Reason != "" {
				observation.State, observation.Reason = "failed", failed.Reason
			}
			if s.removalProven(place, generation, read) {
				if err := cleanup(entry.config); err == nil {
					entry.cleaned = true
				} else {
					entry.cleaned = false
					observation.Reason = "cleanup_unverified"
				}
			} else {
				entry.cleaned = false
			}
			if entry.cleaned {
				observation.State = "cleaned"
				observation.CleanupComplete = true
			}
			result.Seats = append(result.Seats, observation)
			continue
		}
		if !entry.ready && entry.failure == "" && !s.clock.Now().Before(entry.deadline) {
			entry.failure = "readiness_deadline_exceeded"
		}
		payload, err := read(fmt.Sprintf("%d.json", place))
		var answer computerSeatAnswer
		valid := err == nil && decodeSeatDocument(payload, &answer) == nil && answer.Version == 1 && answer.Generation == generation && answer.Place == place
		if valid && answer.State == "failed" && answer.Reason != "" {
			entry.failure = answer.Reason
		}
		if valid && answer.State == "ready" && !answer.CleanupComplete && answer.Reason == "" && entry.failure == "" {
			entry.readyGeneration = 0
			// Bound real I/O as well as checking the injected monotonic clock after
			// both greetings. A ready file alone is never readiness evidence.
			budget := entry.deadline.Sub(s.clock.Now())
			if entry.ready {
				budget = 2 * time.Second
			}
			if budget > 0 {
				probeCtx, cancel := context.WithTimeout(ctx, budget)
				err = probe(probeCtx, entry.config.ViewPort)
				if err == nil {
					err = probe(probeCtx, entry.config.ControlPort)
				}
				cancel()
				if !entry.ready && !s.clock.Now().Before(entry.deadline) {
					entry.failure = "readiness_deadline_exceeded"
				} else if err == nil && ctx.Err() == nil {
					entry.ready = true
					entry.readyGeneration = generation
				}
			}
		}
		if entry.failure != "" {
			observation.State, observation.Reason = "failed", entry.failure
		} else if entry.ready && entry.readyGeneration == generation && valid && answer.State == "ready" && answer.Reason == "" && !answer.CleanupComplete {
			observation.State = "ready"
		}
		result.Seats = append(result.Seats, observation)
	}
	return result, nil
}

// Applied proof covers the replaced-before-observed add race. If a per-place
// answer exists, it must agree with successful removal. The current applied
// proof carries that absence forward when omitted answers were not rewritten.
func (s *computerSeats) removalProven(place int, generation uint64, read func(string) ([]byte, error)) bool {
	payload, err := read("applied.json")
	if err != nil {
		return false
	}
	var applied struct {
		Version         int    `json:"version"`
		Generation      uint64 `json:"generation"`
		WantedPlaces    []int  `json:"wanted_places"`
		CleanupComplete bool   `json:"cleanup_complete"`
	}
	if decodeSeatDocument(payload, &applied) != nil || applied.Version != 1 || applied.Generation != generation || !applied.CleanupComplete || applied.WantedPlaces == nil {
		return false
	}
	wanted := make([]int, 0, len(s.wanted))
	for _, seat := range s.wanted {
		wanted = append(wanted, seat.Place)
	}
	if !slices.Equal(wanted, applied.WantedPlaces) || slices.Contains(applied.WantedPlaces, place) {
		return false
	}
	payload, err = read(fmt.Sprintf("%d.json", place))
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	var answer computerSeatAnswer
	return err == nil && decodeSeatDocument(payload, &answer) == nil && answer.Version == 1 && answer.Generation > 0 && answer.Generation <= generation && answer.Place == place && answer.State == "removed" && answer.Reason == "" && answer.CleanupComplete
}
