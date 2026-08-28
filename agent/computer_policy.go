package agent

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

type ComputerPolicyRevocationReason string

const (
	ComputerPolicyRevoked           ComputerPolicyRevocationReason = "policy_revoked"
	ComputerPolicyWatchLost         ComputerPolicyRevocationReason = "watch_lost"
	ComputerPolicyExpired           ComputerPolicyRevocationReason = "policy_expired"
	ComputerPolicyGenerationChanged ComputerPolicyRevocationReason = "policy_generation_changed"
	ComputerPolicyRevisionRegressed ComputerPolicyRevocationReason = "policy_revision_regressed"
)

type ComputerPolicyRevocation struct {
	Reason         ComputerPolicyRevocationReason
	PolicyRevision int64
	Permission     l1.ComputerGrantPermission
}

type computerGrantDecision struct {
	ComputerID     string
	FabricID       string
	UserID         string
	Permission     l1.ComputerGrantPermission
	Administrator  bool
	PolicyRevision int64
	FreshUntil     time.Time
}

func (decision computerGrantDecision) allowed() bool {
	return decision.Permission == l1.ComputerGrantView || decision.Permission == l1.ComputerGrantControl
}

// ComputerGrantAuthorization is the race-free bridge for #178: lookup and
// revocation registration occur under one lock. A future Take-over session
// must release it only after both relay legs have closed; policy install
// acknowledgement waits for that release.
type ComputerGrantAuthorization struct {
	cache       *ComputerPolicyCache
	id          uint64
	decision    computerGrantDecision
	revocations chan ComputerPolicyRevocation
	once        sync.Once
	barriers    []*computerPolicyDrainBarrier
	revoked     bool
}

type ComputerAdmissionRole string

const ComputerAdmissionView ComputerAdmissionRole = "view"

// AdmissionRole deliberately cannot express control. Every connection starts
// as view; a later session-bound Take may use CanTake separately.
func (authorization *ComputerGrantAuthorization) AdmissionRole() ComputerAdmissionRole {
	return ComputerAdmissionView
}

func (authorization *ComputerGrantAuthorization) AuthorizedRole() l1.ComputerGrantPermission {
	if authorization == nil {
		return l1.ComputerGrantNone
	}
	return authorization.decision.Permission
}

func (authorization *ComputerGrantAuthorization) PolicyRevision() int64 {
	if authorization == nil {
		return 0
	}
	return authorization.decision.PolicyRevision
}

func (authorization *ComputerGrantAuthorization) CanTake() bool {
	return authorization != nil && authorization.decision.Permission == l1.ComputerGrantControl
}

func (authorization *ComputerGrantAuthorization) IsAdministrator() bool {
	return authorization != nil && authorization.decision.Administrator
}

func (authorization *ComputerGrantAuthorization) Revocations() <-chan ComputerPolicyRevocation {
	if authorization == nil {
		closed := make(chan ComputerPolicyRevocation)
		close(closed)
		return closed
	}
	return authorization.revocations
}

func (authorization *ComputerGrantAuthorization) Release() {
	if authorization == nil || authorization.cache == nil {
		return
	}
	authorization.once.Do(func() { authorization.cache.release(authorization.id) })
}

type ComputerPolicyInstallReceipt struct {
	Acknowledgement l1.ComputerPolicyInstallAcknowledgement
	SessionsClosed  <-chan struct{}
}

type ComputerSubmissionAuthority struct {
	ComputerID           string
	Enabled              bool
	SubmitIntentRevision int64
	SubmitMaxInflight    int
	SubmitPolicyRevision int64
}

type computerSubmissionSubscription struct {
	computerID string
	updates    chan ComputerSubmissionAuthority
}

type computerPolicyDrainBarrier struct {
	mu        sync.Mutex
	remaining int
	done      chan struct{}
}

func newComputerPolicyDrainBarrier(count int) *computerPolicyDrainBarrier {
	barrier := &computerPolicyDrainBarrier{remaining: count, done: make(chan struct{})}
	if count == 0 {
		close(barrier.done)
	}
	return barrier
}

func (barrier *computerPolicyDrainBarrier) release() {
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	if barrier.remaining == 0 {
		return
	}
	barrier.remaining--
	if barrier.remaining == 0 {
		close(barrier.done)
	}
}

// ComputerPolicyCache contains no durable state. A process restart therefore
// begins closed and can become usable only from the current boot's heartbeat
// bootstrap or watch.
type ComputerPolicyCache struct {
	mu             sync.Mutex
	clock          Clock
	nodeID         string
	bootSessionID  string
	snapshot       l1.ComputerPolicySnapshot
	valid          bool
	highGeneration int64
	highRevision   int64
	installSerial  uint64
	nextAuthID     uint64
	nextSubmitID   uint64
	authorizations map[uint64]*ComputerGrantAuthorization
	submitWatchers map[uint64]computerSubmissionSubscription
	closed         chan struct{}
	closeOnce      sync.Once
}

func NewComputerPolicyCache(clock Clock, nodeID, bootSessionID string) *ComputerPolicyCache {
	return &ComputerPolicyCache{clock: clock, nodeID: nodeID, bootSessionID: bootSessionID,
		authorizations: make(map[uint64]*ComputerGrantAuthorization),
		submitWatchers: make(map[uint64]computerSubmissionSubscription), closed: make(chan struct{})}
}

func (cache *ComputerPolicyCache) SubscribeComputerSubmission(computerID string) (ComputerSubmissionAuthority, <-chan ComputerSubmissionAuthority, func()) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.expireIfNeededLocked()
	cache.nextSubmitID++
	id := cache.nextSubmitID
	updates := make(chan ComputerSubmissionAuthority, 1)
	cache.submitWatchers[id] = computerSubmissionSubscription{computerID: computerID, updates: updates}
	current := cache.submissionAuthorityLocked(computerID)
	return current, updates, func() {
		cache.mu.Lock()
		watcher, ok := cache.submitWatchers[id]
		if ok {
			delete(cache.submitWatchers, id)
			close(watcher.updates)
		}
		cache.mu.Unlock()
	}
}

func (cache *ComputerPolicyCache) submissionAuthorityLocked(computerID string) ComputerSubmissionAuthority {
	authority := ComputerSubmissionAuthority{ComputerID: computerID}
	if !cache.valid {
		return authority
	}
	for _, computer := range cache.snapshot.Computers {
		if computer.ComputerID == computerID {
			return ComputerSubmissionAuthority{ComputerID: computerID, Enabled: computer.SubmitEnabled,
				SubmitIntentRevision: computer.SubmitIntentRevision, SubmitMaxInflight: computer.SubmitMaxInflight,
				SubmitPolicyRevision: computer.SubmitPolicyRevision}
		}
	}
	return authority
}

func (cache *ComputerPolicyCache) notifyComputerSubmissionLocked() {
	for _, watcher := range cache.submitWatchers {
		next := cache.submissionAuthorityLocked(watcher.computerID)
		select {
		case watcher.updates <- next:
		default:
			select {
			case <-watcher.updates:
			default:
			}
			watcher.updates <- next
		}
	}
}

func (cache *ComputerPolicyCache) Revision() int64 {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	return cache.highRevision
}

func (cache *ComputerPolicyCache) Valid() bool {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.expireIfNeededLocked()
	return cache.valid
}

func (cache *ComputerPolicyCache) lookupGrant(computerID string, identity fabric.Identity) computerGrantDecision {
	if !validComputerPolicyPerson(identity) {
		return computerGrantDecision{ComputerID: computerID, FabricID: identity.FabricID,
			UserID: identity.UserID, Permission: l1.ComputerGrantNone}
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.expireIfNeededLocked()
	return cache.lookupLocked(cache.snapshot, cache.valid, computerID, identity)
}

func (cache *ComputerPolicyCache) AcquireGrant(computerID string, identity fabric.Identity) (*ComputerGrantAuthorization, error) {
	if !validComputerPolicyPerson(identity) {
		return nil, errors.New("agent: Computer policy requires a person identity")
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.expireIfNeededLocked()
	decision := cache.lookupLocked(cache.snapshot, cache.valid, computerID, identity)
	if !decision.allowed() {
		return nil, nil
	}
	cache.nextAuthID++
	authorization := &ComputerGrantAuthorization{cache: cache, id: cache.nextAuthID, decision: decision,
		revocations: make(chan ComputerPolicyRevocation, 1)}
	cache.authorizations[authorization.id] = authorization
	return authorization, nil
}

func validComputerPolicyPerson(identity fabric.Identity) bool {
	return identity.Kind != fabric.IdentityKindMachine && identity.FabricID != "" && identity.UserID != "" &&
		identity.DeviceID != "" && identity.FabricID == strings.TrimSpace(identity.FabricID) &&
		identity.UserID == strings.TrimSpace(identity.UserID) && identity.DeviceID == strings.TrimSpace(identity.DeviceID) &&
		len(identity.FabricID) <= 255 && len(identity.UserID) <= 255 && len(identity.DeviceID) <= 255
}

func (cache *ComputerPolicyCache) lookupLocked(snapshot l1.ComputerPolicySnapshot, valid bool, computerID string, identity fabric.Identity) computerGrantDecision {
	decision := computerGrantDecision{ComputerID: computerID, FabricID: identity.FabricID,
		UserID: identity.UserID, Permission: l1.ComputerGrantNone}
	if !valid || identity.Kind == fabric.IdentityKindMachine || identity.FabricID == "" || identity.UserID == "" ||
		identity.FabricID != snapshot.IssuingFabricID {
		return decision
	}
	decision.PolicyRevision = snapshot.PolicyRevision
	decision.FreshUntil = snapshot.FreshUntil
	foundComputer := false
	for _, computer := range snapshot.Computers {
		if computer.ComputerID != computerID {
			continue
		}
		foundComputer = true
		for _, admin := range snapshot.Admins {
			if admin.FabricID == identity.FabricID && admin.UserID == identity.UserID {
				decision.Permission = l1.ComputerGrantControl
				decision.Administrator = true
				return decision
			}
		}
		for _, grant := range computer.Grants {
			if grant.FabricID == identity.FabricID && grant.UserID == identity.UserID {
				decision.Permission = grant.Permission
				return decision
			}
		}
	}
	if !foundComputer {
		return decision
	}
	return decision
}

func (cache *ComputerPolicyCache) Install(snapshot l1.ComputerPolicySnapshot) (ComputerPolicyInstallReceipt, error) {
	if err := l1.ValidateComputerPolicySnapshot(snapshot); err != nil {
		cache.Invalidate(ComputerPolicyWatchLost)
		return ComputerPolicyInstallReceipt{}, fmt.Errorf("agent: validate Computer policy: %w", err)
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if snapshot.NodeID != cache.nodeID || snapshot.BootSessionID != cache.bootSessionID {
		cache.invalidateLocked(ComputerPolicyWatchLost)
		return ComputerPolicyInstallReceipt{}, errors.New("agent: Computer policy is bound to another node boot")
	}
	if !snapshot.FreshUntil.After(cache.clock.Now()) {
		cache.invalidateLocked(ComputerPolicyExpired)
		return ComputerPolicyInstallReceipt{}, errors.New("agent: Computer policy is already expired")
	}
	if snapshot.PolicyGeneration < cache.highGeneration ||
		snapshot.PolicyGeneration == cache.highGeneration && snapshot.PolicyRevision < cache.highRevision {
		cache.invalidateLocked(ComputerPolicyRevisionRegressed)
		return ComputerPolicyInstallReceipt{}, errors.New("agent: Computer policy revision regressed")
	}
	if cache.valid && snapshot.PolicyGeneration == cache.snapshot.PolicyGeneration && snapshot.PolicyRevision == cache.snapshot.PolicyRevision {
		if err := validateSameRevisionPolicy(cache.snapshot, snapshot); err != nil {
			cache.invalidateLocked(ComputerPolicyRevisionRegressed)
			return ComputerPolicyInstallReceipt{}, err
		}
	}
	reason := ComputerPolicyRevoked
	if cache.valid && snapshot.PolicyGeneration != cache.snapshot.PolicyGeneration {
		reason = ComputerPolicyGenerationChanged
	}
	affected := make([]*ComputerGrantAuthorization, 0)
	for _, authorization := range cache.authorizations {
		identity := fabric.Identity{FabricID: authorization.decision.FabricID, UserID: authorization.decision.UserID}
		next := cache.lookupLocked(snapshot, true, authorization.decision.ComputerID, identity)
		if authorization.revoked || computerGrantRank(next.Permission) < computerGrantRank(authorization.decision.Permission) || reason == ComputerPolicyGenerationChanged {
			affected = append(affected, authorization)
			notifyAuthorization(authorization, ComputerPolicyRevocation{Reason: reason,
				PolicyRevision: snapshot.PolicyRevision, Permission: next.Permission})
		}
	}
	barrier := newComputerPolicyDrainBarrier(len(affected))
	for _, authorization := range affected {
		authorization.barriers = append(authorization.barriers, barrier)
	}
	cache.snapshot = snapshot
	cache.valid = true
	cache.highGeneration = snapshot.PolicyGeneration
	cache.highRevision = snapshot.PolicyRevision
	cache.installSerial++
	cache.notifyComputerSubmissionLocked()
	serial := cache.installSerial
	cache.scheduleExpiryLocked(serial, snapshot.FreshUntil)
	return ComputerPolicyInstallReceipt{Acknowledgement: l1.ComputerPolicyInstallAcknowledgement{
		NodeID: snapshot.NodeID, BootSessionID: snapshot.BootSessionID, PolicyGeneration: snapshot.PolicyGeneration,
		PolicyRevision: snapshot.PolicyRevision, SnapshotDigest: snapshot.SnapshotDigest,
	}, SessionsClosed: barrier.done}, nil
}

func validateSameRevisionPolicy(current, next l1.ComputerPolicySnapshot) error {
	currentAdmins := make(map[string]struct{}, len(current.Admins))
	for _, admin := range current.Admins {
		currentAdmins[admin.FabricID+"\x00"+admin.UserID] = struct{}{}
	}
	if len(currentAdmins) != len(next.Admins) {
		return errors.New("agent: Computer policy changed administrators without a revision")
	}
	for _, admin := range next.Admins {
		if _, ok := currentAdmins[admin.FabricID+"\x00"+admin.UserID]; !ok {
			return errors.New("agent: Computer policy changed administrators without a revision")
		}
	}
	type computerAuthority struct {
		grants         map[string]l1.ComputerGrantPermission
		enabled        bool
		intentRevision int64
		maxInflight    int
		policyRevision int64
	}
	currentComputers := make(map[string]computerAuthority, len(current.Computers))
	for _, computer := range current.Computers {
		grants := make(map[string]l1.ComputerGrantPermission, len(computer.Grants))
		for _, grant := range computer.Grants {
			grants[grant.FabricID+"\x00"+grant.UserID] = grant.Permission
		}
		currentComputers[computer.ComputerID] = computerAuthority{grants: grants, enabled: computer.SubmitEnabled,
			intentRevision: computer.SubmitIntentRevision, maxInflight: computer.SubmitMaxInflight,
			policyRevision: computer.SubmitPolicyRevision}
	}
	for _, computer := range next.Computers {
		currentAuthority, existed := currentComputers[computer.ComputerID]
		if !existed {
			continue
		}
		if currentAuthority.enabled != computer.SubmitEnabled ||
			currentAuthority.intentRevision != computer.SubmitIntentRevision ||
			currentAuthority.maxInflight != computer.SubmitMaxInflight ||
			currentAuthority.policyRevision != computer.SubmitPolicyRevision {
			return errors.New("agent: Computer submission authority changed without a revision")
		}
		if len(currentAuthority.grants) != len(computer.Grants) {
			return errors.New("agent: Computer grants changed without a revision")
		}
		for _, grant := range computer.Grants {
			if currentAuthority.grants[grant.FabricID+"\x00"+grant.UserID] != grant.Permission {
				return errors.New("agent: Computer grants changed without a revision")
			}
		}
	}
	return nil
}

func computerGrantRank(permission l1.ComputerGrantPermission) int {
	switch permission {
	case l1.ComputerGrantControl:
		return 2
	case l1.ComputerGrantView:
		return 1
	default:
		return 0
	}
}

func notifyAuthorization(authorization *ComputerGrantAuthorization, revocation ComputerPolicyRevocation) {
	authorization.revoked = true
	select {
	case authorization.revocations <- revocation:
	default:
	}
}

func (cache *ComputerPolicyCache) Invalidate(reason ComputerPolicyRevocationReason) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.invalidateLocked(reason)
}

func (cache *ComputerPolicyCache) invalidateLocked(reason ComputerPolicyRevocationReason) {
	if !cache.valid && len(cache.authorizations) == 0 {
		return
	}
	cache.valid = false
	cache.installSerial++
	cache.notifyComputerSubmissionLocked()
	for _, authorization := range cache.authorizations {
		notifyAuthorization(authorization, ComputerPolicyRevocation{Reason: reason,
			PolicyRevision: cache.snapshot.PolicyRevision, Permission: l1.ComputerGrantNone})
	}
}

func (cache *ComputerPolicyCache) expireIfNeededLocked() {
	if cache.valid && !cache.snapshot.FreshUntil.After(cache.clock.Now()) {
		cache.invalidateLocked(ComputerPolicyExpired)
	}
}

func (cache *ComputerPolicyCache) scheduleExpiryLocked(serial uint64, freshUntil time.Time) {
	delay := freshUntil.Sub(cache.clock.Now())
	if delay < 0 {
		delay = 0
	}
	timer := cache.clock.NewTimer(delay)
	go func() {
		defer stopTimer(timer)
		select {
		case <-cache.closed:
			return
		case <-timer.C():
			cache.mu.Lock()
			if cache.installSerial == serial {
				cache.invalidateLocked(ComputerPolicyExpired)
			}
			cache.mu.Unlock()
		}
	}()
}

func (cache *ComputerPolicyCache) release(id uint64) {
	cache.mu.Lock()
	authorization, ok := cache.authorizations[id]
	if ok {
		delete(cache.authorizations, id)
	}
	cache.mu.Unlock()
	if !ok {
		return
	}
	for _, barrier := range authorization.barriers {
		barrier.release()
	}
	close(authorization.revocations)
}

func (cache *ComputerPolicyCache) Close() {
	cache.closeOnce.Do(func() {
		close(cache.closed)
		cache.Invalidate(ComputerPolicyWatchLost)
		cache.mu.Lock()
		for id, watcher := range cache.submitWatchers {
			delete(cache.submitWatchers, id)
			close(watcher.updates)
		}
		cache.mu.Unlock()
	})
}
