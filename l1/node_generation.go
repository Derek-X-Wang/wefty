package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/hostboot"
	"github.com/Derek-X-Wang/wefty/internal/upgradecompat"
)

// Generation roles are L1 session authority, not installation catalog intent.
type NodeGenerationRole string

const (
	NodeGenerationStaged   NodeGenerationRole = "staged"
	NodeGenerationPrimary  NodeGenerationRole = "primary"
	NodeGenerationRetiring NodeGenerationRole = "retiring"
	NodeGenerationClosed   NodeGenerationRole = "closed"
)

type ResidentAttempt struct {
	JobID               string `json:"job_id"`
	AttemptID           string `json:"attempt_id"`
	BootSessionID       string `json:"boot_session_id"`
	AuthorityGeneration int64  `json:"authority_generation"`
	FencingToken        string `json:"fencing_token"`
}

type NodeGeneration struct {
	AgentGenerationID          string                    `json:"agent_generation_id"`
	BootSessionID              string                    `json:"boot_session_id"`
	AuthorityGeneration        int64                     `json:"authority_generation"`
	Role                       NodeGenerationRole        `json:"role"`
	Registration               contract.NodeRegistration `json:"registration"`
	RetirementProtocolVersions []uint32                  `json:"retirement_protocol_versions"`
	SelectedProtocol           uint32                    `json:"selected_protocol"`
	PromotionOperationID       string                    `json:"promotion_operation_id"`
	CreatedAt                  time.Time                 `json:"created_at"`
	LastHeartbeatAt            time.Time                 `json:"last_heartbeat_at"`
	RetiringAt                 *time.Time                `json:"retiring_at,omitempty"`
	ClosedAt                   *time.Time                `json:"closed_at,omitempty"`
	ResidentAttempts           []ResidentAttempt         `json:"resident_attempts"`
}

type NodeGenerationInventory struct {
	NodeID          string           `json:"node_id"`
	CatalogRevision int64            `json:"catalog_revision"`
	Generations     []NodeGeneration `json:"generations"`
}

type PromoteNodeGenerationRequest struct {
	OperationID                  string            `json:"operation_id"`
	ExpectedPrimaryGenerationID  string            `json:"expected_primary_generation_id"`
	ExpectedPrimaryBootSessionID string            `json:"expected_primary_boot_session_id"`
	ExpectedAuthorityGeneration  int64             `json:"expected_authority_generation"`
	CandidateGenerationID        string            `json:"candidate_generation_id"`
	CandidateBootSessionID       string            `json:"candidate_boot_session_id"`
	ExpectedCatalogRevision      int64             `json:"expected_catalog_revision"`
	CatalogRevision              int64             `json:"catalog_revision"`
	IntentRevision               int64             `json:"intent_revision"`
	SelectedProtocol             uint32            `json:"selected_protocol"`
	RetiringGenerationIDs        []string          `json:"retiring_generation_ids"`
	ResidentAttempts             []ResidentAttempt `json:"resident_attempts"`
}

type NodeGenerationHeartbeatRequest struct {
	BootSessionID string `json:"boot_session_id"`
}

type CloseNodeGenerationRequest struct {
	Abort                *StagedGenerationAbort `json:"abort,omitempty"`
	BootSessionID        string                 `json:"boot_session_id"`
	PromotionOperationID string                 `json:"promotion_operation_id"`
	ResidentAttempts     []ResidentAttempt      `json:"resident_attempts"`
	RuntimesReaped       bool                   `json:"runtimes_reaped"`
	EvidenceSettled      bool                   `json:"evidence_settled"`
}

func (s *Store) initializeNodeGenerations(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
 CREATE TABLE IF NOT EXISTS node_generation_catalog (
 node_id TEXT PRIMARY KEY REFERENCES nodes(node_id), catalog_revision INTEGER NOT NULL DEFAULT 0 CHECK(catalog_revision>=0));
 CREATE TABLE IF NOT EXISTS node_generations (
 node_id TEXT NOT NULL REFERENCES nodes(node_id), generation_id TEXT NOT NULL,
 boot_session_id TEXT NOT NULL, authority_generation INTEGER NOT NULL,
 role TEXT NOT NULL CHECK(role IN ('staged','primary','retiring','closed')),
 registration_json BLOB NOT NULL, protocols_json BLOB NOT NULL,
 selected_protocol INTEGER NOT NULL DEFAULT 0, promotion_operation_id TEXT NOT NULL DEFAULT '',
 created_ns INTEGER NOT NULL, heartbeat_ns INTEGER NOT NULL, retiring_ns INTEGER, closed_ns INTEGER,
 PRIMARY KEY(node_id,generation_id), UNIQUE(node_id,boot_session_id));
 CREATE UNIQUE INDEX IF NOT EXISTS node_generations_primary ON node_generations(node_id) WHERE role='primary';
 CREATE TABLE IF NOT EXISTS node_generation_residents (
 node_id TEXT NOT NULL, generation_id TEXT NOT NULL, attempt_id TEXT NOT NULL UNIQUE,
 job_id TEXT NOT NULL, boot_session_id TEXT NOT NULL, authority_generation INTEGER NOT NULL, fencing_token TEXT NOT NULL,
 PRIMARY KEY(node_id,generation_id,attempt_id),
 FOREIGN KEY(node_id,generation_id) REFERENCES node_generations(node_id,generation_id));
 CREATE TABLE IF NOT EXISTS node_generation_promotions (
 node_id TEXT NOT NULL REFERENCES nodes(node_id), operation_id TEXT NOT NULL, request_json BLOB NOT NULL, result_json BLOB NOT NULL,
 PRIMARY KEY(node_id,operation_id));`)
	return err
}

func retirementProtocols(capabilities map[string]bool) ([]uint32, error) {
	versions := []uint32{}
	for name, enabled := range capabilities {
		name = strings.ToLower(strings.TrimSpace(name))
		if !strings.HasPrefix(name, "upgrade:retirement:") {
			continue
		}
		suffix := strings.TrimPrefix(name, "upgrade:retirement:v")
		value, err := strconv.ParseUint(suffix, 10, 32)
		if err != nil || value == 0 || strconv.FormatUint(value, 10) != suffix {
			return nil, protocolError(contract.ErrorInvalidRequest, "malformed retirement protocol capability")
		}
		if enabled {
			versions = append(versions, uint32(value))
		}
	}
	slices.Sort(versions)
	return versions, nil
}

// guardGenerationRegistration runs before any Node fact is changed. Legacy
// registrations remain unchanged until this Node opts into generation identity.
func guardGenerationRegistration(ctx context.Context, tx *sql.Tx, registration contract.NodeRegistration) error {
	if registration.SystemBootID != "" && !hostboot.ValidIdentity(registration.SystemBootID) {
		return stagedAbortConflict("invalid generation system boot identity")
	}
	var generation string
	err := tx.QueryRowContext(ctx, `SELECT generation_id FROM node_generations WHERE node_id=? AND role='primary'`, registration.NodeID).Scan(&generation)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil && generation != registration.AgentGenerationID {
		return protocolError(contract.ErrorNodeSessionReplaced, "only the recorded primary generation may register")
	}
	if registration.AgentGenerationID == "" {
		return nil
	}
	var role NodeGenerationRole
	var priorJSON []byte
	e := tx.QueryRowContext(ctx, `SELECT role,registration_json FROM node_generations WHERE node_id=? AND generation_id=?`, registration.NodeID, registration.AgentGenerationID).Scan(&role, &priorJSON)
	if e == nil {
		var prior contract.NodeRegistration
		if err := json.Unmarshal(priorJSON, &prior); err != nil {
			return err
		}
		if prior.SystemBootID != "" && (registration.SystemBootID == "" || (registration.BootSessionID == prior.BootSessionID && !hostboot.SameBoot(registration.SystemBootID, prior.SystemBootID))) {
			return stagedAbortConflict("generation system boot binding changed or was withdrawn")
		}
	}
	if e == nil && role != NodeGenerationPrimary {
		return protocolError(contract.ErrorNodeSessionReplaced, "generation cannot register as primary")
	}
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	if _, e = retirementProtocols(registration.Capabilities); e != nil {
		return e
	}
	// Never adopt a legacy session's live work into a generation.
	if errors.Is(err, sql.ErrNoRows) {
		var active bool
		if e = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM attempts WHERE node_id=? AND state IN ('claimed','running','awaiting-input'))`, registration.NodeID).Scan(&active); e != nil {
			return e
		}
		if active {
			return protocolError(contract.ErrorConflict, "legacy resident Attempts must settle before generation enrollment")
		}
	}
	return nil
}

func recordPrimaryGeneration(ctx context.Context, tx *sql.Tx, registration contract.NodeRegistration, now time.Time) error {
	if registration.AgentGenerationID == "" {
		return nil
	}
	registration.SystemBootID = canonicalGenerationSystemBoot(registration.SystemBootID)
	protocols, err := retirementProtocols(registration.Capabilities)
	if err != nil {
		return err
	}
	var oldBoot string
	var oldAuthority int64
	err = tx.QueryRowContext(ctx, `SELECT boot_session_id,authority_generation FROM node_generations WHERE node_id=? AND generation_id=?`, registration.NodeID, registration.AgentGenerationID).Scan(&oldBoot, &oldAuthority)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	// Rejoining the same generation/boot must not revoke its resident Attempts.
	if err == nil && oldBoot == registration.BootSessionID {
		// First observation from an already-promoted primary can bind legacy
		// cohort metadata on this unchanged boot. A new boot never adopts it.
		if registration.SystemBootID != "" {
			if _, err = tx.ExecContext(ctx, `UPDATE node_generations SET registration_json=json_set(CAST(registration_json AS TEXT),'$.system_boot_id',?) WHERE node_id=? AND role IN ('primary','retiring') AND COALESCE(json_extract(registration_json,'$.system_boot_id'),'')=''`, registration.SystemBootID, registration.NodeID); err != nil {
				return err
			}
		}
		if _, err = tx.ExecContext(ctx, `UPDATE nodes SET authority_generation=? WHERE node_id=?`, oldAuthority, registration.NodeID); err != nil {
			return err
		}
	}
	var authority int64
	var capabilities []byte
	if err = tx.QueryRowContext(ctx, `SELECT authority_generation,capabilities_json FROM nodes WHERE node_id=?`, registration.NodeID).Scan(&authority, &capabilities); err != nil {
		return err
	}
	if err = json.Unmarshal(capabilities, &registration.Capabilities); err != nil {
		return err
	}
	protocols, err = retirementProtocols(registration.Capabilities)
	if err != nil {
		return err
	}
	payload, _ := json.Marshal(registration)
	versions, _ := json.Marshal(protocols)
	_, err = tx.ExecContext(ctx, `INSERT INTO node_generations(node_id,generation_id,boot_session_id,authority_generation,role,registration_json,protocols_json,created_ns,heartbeat_ns)
 VALUES(?,?,?,?,'primary',?,?,?,?) ON CONFLICT(node_id,generation_id) DO UPDATE SET boot_session_id=excluded.boot_session_id,authority_generation=excluded.authority_generation,registration_json=excluded.registration_json,protocols_json=excluded.protocols_json,heartbeat_ns=excluded.heartbeat_ns`, registration.NodeID, registration.AgentGenerationID, registration.BootSessionID, authority, payload, versions, now.UnixNano(), now.UnixNano())
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO node_generation_catalog(node_id) VALUES(?) ON CONFLICT DO NOTHING`, registration.NodeID)
	return err
}

func requireGenerationIdentity(ctx context.Context, q queryer, nodeID, identity string) error {
	var stored string
	err := q.QueryRowContext(ctx, `SELECT identity_node_id FROM nodes WHERE node_id=?`, nodeID).Scan(&stored)
	if errors.Is(err, sql.ErrNoRows) {
		return protocolError(contract.ErrorNodeNotRegistered, "node is not registered")
	}
	if err != nil {
		return err
	}
	if stored != identity {
		return protocolError(contract.ErrorIdentityBound, "node is bound to another identity")
	}
	return nil
}

// StageNodeGeneration records a ready candidate without granting any authority.
// The activation adapter must verify readiness before making this assertion.
func (s *Store) StageNodeGeneration(ctx context.Context, identity string, registration contract.NodeRegistration) (NodeGenerationInventory, error) {
	if registration.AgentGenerationID == "" || registration.BootSessionID == "" || registration.AgentVersion == "" || registration.OS != "darwin" || registration.Architecture == "" {
		return NodeGenerationInventory{}, protocolError(contract.ErrorInvalidRequest, "a complete plain Mac generation registration is required")
	}
	if registration.SystemBootID != "" && !hostboot.ValidIdentity(registration.SystemBootID) {
		return NodeGenerationInventory{}, stagedAbortConflict("invalid candidate system boot identity")
	}
	registration.SystemBootID = canonicalGenerationSystemBoot(registration.SystemBootID)
	incoming, err := canonicalCapabilityObservation(registrationCapabilityObservation(registration))
	if err != nil {
		return NodeGenerationInventory{}, err
	}
	registration.Capabilities = incoming.observation.Capabilities
	registration.MissingCapabilities = incoming.observation.MissingCapabilities
	registration.CapabilityObservedAt = incoming.observation.ObservedAt
	protocols, err := retirementProtocols(registration.Capabilities)
	if err != nil {
		return NodeGenerationInventory{}, err
	}
	if !slices.Contains(protocols, upgradecompat.ProtocolV1) {
		return NodeGenerationInventory{}, protocolError(contract.ErrorConflict, "candidate does not support retirement protocol 1")
	}
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return NodeGenerationInventory{}, err
	}
	defer write.rollback()
	tx := write.tx
	if err = requireGenerationIdentity(ctx, tx, registration.NodeID, identity); err != nil {
		return NodeGenerationInventory{}, err
	}
	inventory, err := readNodeGenerations(ctx, tx, registration.NodeID)
	if err != nil {
		return inventory, err
	}
	payload, _ := json.Marshal(registration)
	versions, _ := json.Marshal(protocols)
	for _, g := range inventory.Generations {
		if g.AgentGenerationID == registration.AgentGenerationID {
			if g.Role != NodeGenerationStaged || !reflect.DeepEqual(g.Registration, registration) {
				return inventory, protocolError(contract.ErrorConflict, "candidate identity already used")
			}
			return inventory, nil
		}
	}
	primary := false
	for _, g := range inventory.Generations {
		if g.Role == NodeGenerationPrimary {
			primary = true
		}
	}
	if !primary {
		return inventory, protocolError(contract.ErrorConflict, "primary must enroll before staging")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO node_generations(node_id,generation_id,boot_session_id,authority_generation,role,registration_json,protocols_json,created_ns,heartbeat_ns) VALUES(?,?,?,0,'staged',?,?,?,?)`, registration.NodeID, registration.AgentGenerationID, registration.BootSessionID, payload, versions, write.at.UnixNano(), write.at.UnixNano())
	if err != nil {
		return inventory, internalError(err, "stage generation")
	}
	inventory, err = readNodeGenerations(ctx, tx, registration.NodeID)
	if err != nil {
		return inventory, err
	}
	return inventory, tx.Commit()
}

// NodeGenerations reads all sessions, including closed sessions retained for
// monotonic replay. Callers must not treat a closed session as a live survivor.
func (s *Store) NodeGenerations(ctx context.Context, identity, nodeID string) (NodeGenerationInventory, error) {
	tx, err := s.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return NodeGenerationInventory{}, err
	}
	defer tx.Rollback()
	if err = requireGenerationIdentity(ctx, tx, nodeID, identity); err != nil {
		return NodeGenerationInventory{}, err
	}
	inventory, err := readNodeGenerations(ctx, tx, nodeID)
	if err != nil {
		return inventory, err
	}
	return inventory, tx.Commit()
}

func readNodeGenerations(ctx context.Context, q queryer, nodeID string) (NodeGenerationInventory, error) {
	return readNodeGenerationInventory(ctx, q, nodeID, nil)
}

// Diagnostic projections retain malformed record identity and continue. Private
// authority reads keep their fail-closed validation through the nil error map.
func readNodeGenerationInventory(ctx context.Context, q queryer, nodeID string, recordErrors map[string]string) (NodeGenerationInventory, error) {
	inventory := NodeGenerationInventory{NodeID: nodeID, Generations: []NodeGeneration{}}
	err := q.QueryRowContext(ctx, `SELECT catalog_revision FROM node_generation_catalog WHERE node_id=?`, nodeID).Scan(&inventory.CatalogRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return inventory, nil
	}
	if err != nil {
		return inventory, err
	}
	rows, err := q.QueryContext(ctx, `SELECT generation_id,boot_session_id,authority_generation,role,registration_json,protocols_json,selected_protocol,promotion_operation_id,created_ns,heartbeat_ns,retiring_ns,closed_ns FROM node_generations WHERE node_id=? ORDER BY generation_id`, nodeID)
	if err != nil {
		return inventory, err
	}
	for rows.Next() {
		var g NodeGeneration
		var payload, versions []byte
		var created, heartbeat int64
		var retiring, closed sql.NullInt64
		if err = rows.Scan(&g.AgentGenerationID, &g.BootSessionID, &g.AuthorityGeneration, &g.Role, &payload, &versions, &g.SelectedProtocol, &g.PromotionOperationID, &created, &heartbeat, &retiring, &closed); err != nil {
			if recordErrors == nil {
				rows.Close()
				return inventory, err
			}
			recordErrors[g.AgentGenerationID] = fmt.Sprintf("generation %s: unreadable retained generation: %v", g.AgentGenerationID, err)
			inventory.Generations = append(inventory.Generations, g)
			continue
		}
		if err = json.Unmarshal(payload, &g.Registration); err != nil {
			if recordErrors == nil {
				rows.Close()
				return inventory, err
			}
			recordErrors[g.AgentGenerationID] = fmt.Sprintf("generation %s: unreadable registration: %v", g.AgentGenerationID, err)
			g.Registration = contract.NodeRegistration{}
		}
		if err = json.Unmarshal(versions, &g.RetirementProtocolVersions); err != nil {
			if recordErrors == nil {
				rows.Close()
				return inventory, err
			}
			cause := fmt.Sprintf("generation %s: unreadable retirement protocols: %v", g.AgentGenerationID, err)
			if recordErrors[g.AgentGenerationID] != "" {
				cause = recordErrors[g.AgentGenerationID] + "; " + cause
			}
			recordErrors[g.AgentGenerationID] = cause
			g.RetirementProtocolVersions = nil
		}
		g.CreatedAt = time.Unix(0, created).UTC()
		g.LastHeartbeatAt = time.Unix(0, heartbeat).UTC()
		if retiring.Valid {
			v := time.Unix(0, retiring.Int64).UTC()
			g.RetiringAt = &v
		}
		if closed.Valid {
			v := time.Unix(0, closed.Int64).UTC()
			g.ClosedAt = &v
		}
		inventory.Generations = append(inventory.Generations, g)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return inventory, err
	}
	for i := range inventory.Generations {
		g := &inventory.Generations[i]
		if g.Role == NodeGenerationPrimary {
			g.ResidentAttempts, err = readPrimaryResidents(ctx, q, nodeID, g.BootSessionID, g.AuthorityGeneration)
		} else {
			g.ResidentAttempts, err = readGenerationResidents(ctx, q, nodeID, g.AgentGenerationID)
		}
		if err != nil {
			return inventory, err
		}
	}
	return inventory, nil
}

func readGenerationResidents(ctx context.Context, q queryer, nodeID, generation string) ([]ResidentAttempt, error) {
	rows, err := q.QueryContext(ctx, `SELECT job_id,attempt_id,boot_session_id,authority_generation,fencing_token FROM node_generation_residents WHERE node_id=? AND generation_id=? ORDER BY attempt_id`, nodeID, generation)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	residents := []ResidentAttempt{}
	for rows.Next() {
		var r ResidentAttempt
		if err = rows.Scan(&r.JobID, &r.AttemptID, &r.BootSessionID, &r.AuthorityGeneration, &r.FencingToken); err != nil {
			return nil, err
		}
		residents = append(residents, r)
	}
	return residents, rows.Err()
}

func canonicalResidents(residents []ResidentAttempt) []ResidentAttempt {
	out := append([]ResidentAttempt{}, residents...)
	slices.SortFunc(out, func(a, b ResidentAttempt) int { return strings.Compare(a.AttemptID, b.AttemptID) })
	return out
}

// retiringAttemptAuthoritySQL deliberately checks exact original membership.
// It does not alter current Job binding, phase, Lease, fence or O/R rules.
const retiringAttemptAuthoritySQL = `EXISTS(SELECT 1 FROM node_generation_residents resident
 JOIN node_generations session ON session.node_id=resident.node_id AND session.generation_id=resident.generation_id
 WHERE resident.attempt_id=a.attempt_id AND resident.node_id=a.node_id
 AND resident.job_id=a.job_id AND resident.boot_session_id=a.boot_session_id
 AND resident.authority_generation=a.authority_generation AND resident.fencing_token=a.fencing_token
 AND session.role='retiring' AND session.boot_session_id=a.boot_session_id AND session.authority_generation=a.authority_generation
 AND COALESCE(json_extract(session.registration_json,'$.system_boot_id'),'')=COALESCE((SELECT json_extract(primary_session.registration_json,'$.system_boot_id') FROM node_generations primary_session WHERE primary_session.node_id=session.node_id AND primary_session.role='primary'),'') )`

func (s *Store) PromoteNodeGeneration(ctx context.Context, identity, nodeID string, request PromoteNodeGenerationRequest) (NodeGenerationInventory, error) {
	if request.OperationID == "" || request.ExpectedPrimaryGenerationID == "" || request.CandidateGenerationID == "" || request.ExpectedPrimaryGenerationID == request.CandidateGenerationID || request.ExpectedCatalogRevision < 0 || request.CatalogRevision <= request.ExpectedCatalogRevision || request.CatalogRevision != request.ExpectedCatalogRevision+1 || request.SelectedProtocol != upgradecompat.ProtocolV1 {
		return NodeGenerationInventory{}, protocolError(contract.ErrorInvalidRequest, "invalid generation promotion fences")
	}
	request.ResidentAttempts = canonicalResidents(request.ResidentAttempts)
	request.RetiringGenerationIDs = append([]string{}, request.RetiringGenerationIDs...)
	slices.Sort(request.RetiringGenerationIDs)
	payload, _ := json.Marshal(request)
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return NodeGenerationInventory{}, err
	}
	defer write.rollback()
	tx := write.tx
	if err = requireGenerationIdentity(ctx, tx, nodeID, identity); err != nil {
		return NodeGenerationInventory{}, err
	}
	var previous, result []byte
	err = tx.QueryRowContext(ctx, `SELECT request_json,result_json FROM node_generation_promotions WHERE node_id=? AND operation_id=?`, nodeID, request.OperationID).Scan(&previous, &result)
	if err == nil {
		if string(previous) != string(payload) {
			return NodeGenerationInventory{}, protocolError(contract.ErrorConflict, "promotion operation identity reused")
		}
		var inventory NodeGenerationInventory
		err = json.Unmarshal(result, &inventory)
		return inventory, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return NodeGenerationInventory{}, err
	}
	inventory, err := readNodeGenerations(ctx, tx, nodeID)
	if err != nil {
		return inventory, err
	}
	var primary, candidate *NodeGeneration
	retiring := []string{}
	live := []upgradecompat.Generation{}
	for i := range inventory.Generations {
		g := &inventory.Generations[i]
		switch g.Role {
		case NodeGenerationPrimary:
			primary = g
		case NodeGenerationStaged:
			if g.AgentGenerationID == request.CandidateGenerationID {
				candidate = g
			}
		case NodeGenerationRetiring:
			retiring = append(retiring, g.AgentGenerationID)
		}
		if g.Role == NodeGenerationPrimary || g.Role == NodeGenerationRetiring {
			live = append(live, upgradecompat.Generation{ID: g.AgentGenerationID, Role: upgradecompat.Role(g.Role), Protocols: g.RetirementProtocolVersions})
		}
	}
	if primary == nil || candidate == nil || primary.AgentGenerationID != request.ExpectedPrimaryGenerationID || primary.BootSessionID != request.ExpectedPrimaryBootSessionID || primary.AuthorityGeneration != request.ExpectedAuthorityGeneration || candidate.BootSessionID != request.CandidateBootSessionID || inventory.CatalogRevision != request.ExpectedCatalogRevision || !reflect.DeepEqual(retiring, request.RetiringGenerationIDs) {
		return inventory, protocolError(contract.ErrorConflict, "generation inventory changed")
	}
	var boot, os string
	var authority, intent int64
	var claims bool
	if err = tx.QueryRowContext(ctx, `SELECT boot_session_id,authority_generation,intent_revision,claims_enabled,os FROM nodes WHERE node_id=?`, nodeID).Scan(&boot, &authority, &intent, &claims, &os); err != nil {
		return inventory, err
	}
	if claims || intent != request.IntentRevision || boot != primary.BootSessionID || authority != primary.AuthorityGeneration || os != "darwin" {
		return inventory, protocolError(contract.ErrorConflict, "promotion requires current primary and fenced closed admission")
	}
	// Freeze only the still-active Attempts of the exact predecessor session.
	residents, err := readPrimaryResidents(ctx, tx, nodeID, boot, authority)
	if err != nil {
		return inventory, err
	}
	if !reflect.DeepEqual(residents, request.ResidentAttempts) {
		return inventory, protocolError(contract.ErrorConflict, "resident Attempt inventory changed")
	}
	work := []upgradecompat.WorkKind{}
	for _, r := range residents {
		var kind string
		var service bool
		if err = tx.QueryRowContext(ctx, `SELECT json_extract(CAST(spec_json AS TEXT),'$.kind'),EXISTS(SELECT 1 FROM service_jobs WHERE service_jobs.job_id=jobs.job_id) FROM jobs WHERE job_id=?`, r.JobID).Scan(&kind, &service); err != nil {
			return inventory, err
		}
		if service || kind != contract.JobKindProcess {
			return inventory, protocolError(contract.ErrorConflict, "resident services and OCI must quiesce before promotion")
		}
		work = append(work, upgradecompat.ProcessOneshot)
	}
	decision := upgradecompat.Decide(upgradecompat.Input{Installation: upgradecompat.PlainMac, CandidateProtocols: candidate.RetirementProtocolVersions, Live: live, Work: work})
	if decision.Mode != upgradecompat.Rolling || decision.Protocol != request.SelectedProtocol {
		return inventory, protocolError(contract.ErrorConflict, "cohort retirement protocol is incompatible")
	}
	for _, g := range inventory.Generations {
		if g.Role == NodeGenerationRetiring && g.SelectedProtocol != request.SelectedProtocol {
			return inventory, protocolError(contract.ErrorConflict, "cohort protocol transition requires acknowledgment")
		}
	}
	for _, r := range residents {
		if _, err = tx.ExecContext(ctx, `INSERT INTO node_generation_residents(node_id,generation_id,attempt_id,job_id,boot_session_id,authority_generation,fencing_token) VALUES(?,?,?,?,?,?,?)`, nodeID, primary.AgentGenerationID, r.AttemptID, r.JobID, r.BootSessionID, r.AuthorityGeneration, r.FencingToken); err != nil {
			return inventory, err
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE node_generations SET role='retiring',retiring_ns=?,selected_protocol=?,promotion_operation_id=? WHERE node_id=? AND generation_id=?`, write.at.UnixNano(), request.SelectedProtocol, request.OperationID, nodeID, primary.AgentGenerationID); err != nil {
		return inventory, err
	}
	if candidate.Registration.SystemBootID != "" {
		for _, g := range inventory.Generations {
			if g.Role != NodeGenerationPrimary && g.Role != NodeGenerationRetiring {
				continue
			}
			if g.Registration.SystemBootID != "" && g.Registration.SystemBootID != candidate.Registration.SystemBootID {
				return inventory, stagedAbortConflict("candidate and surviving cohort system boots differ")
			}
			if g.Registration.SystemBootID == "" {
				g.Registration.SystemBootID = candidate.Registration.SystemBootID
				body, _ := json.Marshal(g.Registration)
				if _, err = tx.ExecContext(ctx, `UPDATE node_generations SET registration_json=? WHERE node_id=? AND generation_id=?`, body, nodeID, g.AgentGenerationID); err != nil {
					return inventory, err
				}
			}
		}
	}
	registration := candidate.Registration
	incoming, err := canonicalCapabilityObservation(registrationCapabilityObservation(registration))
	if err != nil {
		return inventory, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE nodes SET boot_session_id=?,authority_generation=authority_generation+1,connect_host=?,root_instance_id=?,os=?,architecture=?,agent_version=?,capabilities_json=?,capability_revision=?,capability_observed_ns=?,missing_capabilities_json=?,capability_reason_code=?,state='alive',last_heartbeat_ns=? WHERE node_id=?`, registration.BootSessionID, registration.ConnectHost, registration.RootInstanceID, registration.OS, registration.Architecture, registration.AgentVersion, incoming.capabilitiesJSON, incoming.observation.Revision, incoming.observation.ObservedAt.UnixNano(), incoming.missingJSON, incoming.observation.ReasonCode, write.at.UnixNano(), nodeID); err != nil {
		return inventory, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE node_generations SET role='primary',authority_generation=?,selected_protocol=?,promotion_operation_id=?,heartbeat_ns=? WHERE node_id=? AND generation_id=?`, authority+1, request.SelectedProtocol, request.OperationID, write.at.UnixNano(), nodeID, candidate.AgentGenerationID); err != nil {
		return inventory, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE node_generation_catalog SET catalog_revision=? WHERE node_id=?`, request.CatalogRevision, nodeID); err != nil {
		return inventory, err
	}
	inventory, err = readNodeGenerations(ctx, tx, nodeID)
	if err != nil {
		return inventory, err
	}
	result, _ = json.Marshal(inventory)
	if _, err = tx.ExecContext(ctx, `INSERT INTO node_generation_promotions(node_id,operation_id,request_json,result_json) VALUES(?,?,?,?)`, nodeID, request.OperationID, payload, result); err != nil {
		return inventory, err
	}
	return inventory, tx.Commit()
}

// HeartbeatRetiringNodeGeneration observes only the original retiring process.
// Renewal health remains per Attempt; this never refreshes primary Node facts.
func (s *Store) HeartbeatRetiringNodeGeneration(ctx context.Context, identity, nodeID, generation, boot string) (NodeGenerationInventory, error) {
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return NodeGenerationInventory{}, err
	}
	defer write.rollback()
	tx := write.tx
	if err = requireGenerationIdentity(ctx, tx, nodeID, identity); err != nil {
		return NodeGenerationInventory{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE node_generations SET heartbeat_ns=? WHERE node_id=? AND generation_id=? AND boot_session_id=? AND role='retiring' AND COALESCE(json_extract(registration_json,'$.system_boot_id'),'')=COALESCE((SELECT json_extract(p.registration_json,'$.system_boot_id') FROM node_generations p WHERE p.node_id=node_generations.node_id AND p.role='primary'),'')`, write.at.UnixNano(), nodeID, generation, boot)
	if err != nil {
		return NodeGenerationInventory{}, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return NodeGenerationInventory{}, err
	}
	if n != 1 {
		return NodeGenerationInventory{}, protocolError(contract.ErrorNodeSessionReplaced, "retiring session is not authorized")
	}
	inventory, err := readNodeGenerations(ctx, tx, nodeID)
	if err != nil {
		return inventory, err
	}
	return inventory, tx.Commit()
}

// nodeGenerationClosureReady is L1's part of the shared #1176 boundary. An
// unaccepted buffered completion and a custody receipt never satisfy it.
func nodeGenerationClosureReady(ctx context.Context, q queryer, nodeID, generation string, now time.Time) (bool, error) {
	var pending bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(
        SELECT 1 FROM node_generation_residents resident
        LEFT JOIN attempts a ON a.attempt_id=resident.attempt_id
        LEFT JOIN jobs j ON j.job_id=resident.job_id
        WHERE resident.node_id=? AND resident.generation_id=?
        AND (a.attempt_id IS NULL OR j.job_id IS NULL OR (
            (a.completion_key IS NULL OR a.state NOT IN ('succeeded','failed','interrupted'))
            AND a.state<>'lost' AND a.lease_expires_ns>?
            AND NOT (j.outcome='canceled' AND j.cancel_settle_by_ns<=?)
        )))`, nodeID, generation, now.UnixNano(), now.UnixNano()).Scan(&pending)
	return !pending, err
}

func (s *Store) CloseNodeGeneration(ctx context.Context, identity, nodeID, generation string, request CloseNodeGenerationRequest) (NodeGenerationInventory, error) {
	if request.Abort != nil {
		return s.abortStagedGeneration(ctx, identity, nodeID, generation, request)
	}
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return NodeGenerationInventory{}, err
	}
	defer write.rollback()
	tx := write.tx
	if err = requireGenerationIdentity(ctx, tx, nodeID, identity); err != nil {
		return NodeGenerationInventory{}, err
	}
	inventory, err := readNodeGenerations(ctx, tx, nodeID)
	if err != nil {
		return inventory, err
	}
	var target *NodeGeneration
	for i := range inventory.Generations {
		if inventory.Generations[i].AgentGenerationID == generation {
			target = &inventory.Generations[i]
		}
	}
	if target == nil || (target.Role != NodeGenerationRetiring && target.Role != NodeGenerationClosed) || target.BootSessionID != request.BootSessionID || target.PromotionOperationID != request.PromotionOperationID || !reflect.DeepEqual(canonicalResidents(target.ResidentAttempts), canonicalResidents(request.ResidentAttempts)) || !request.RuntimesReaped || !request.EvidenceSettled {
		return inventory, protocolError(contract.ErrorConflict, "retirement requires exact session, positive reap and settled evidence")
	}
	if target.Role == NodeGenerationClosed {
		return inventory, nil
	}
	ready, err := nodeGenerationClosureReady(ctx, tx, nodeID, generation, write.at)
	if err != nil {
		return inventory, err
	}
	if !ready {
		return inventory, protocolError(contract.ErrorConflict, "resident completion must be accepted or Lease ended")
	}
	if _, err = tx.ExecContext(ctx, `UPDATE node_generations SET role='closed',closed_ns=? WHERE node_id=? AND generation_id=?`, write.at.UnixNano(), nodeID, generation); err != nil {
		return inventory, err
	}
	inventory, err = readNodeGenerations(ctx, tx, nodeID)
	if err != nil {
		return inventory, err
	}
	return inventory, tx.Commit()
}

func (s *Server) nodeGenerations(w http.ResponseWriter, r *http.Request) {
	identity := identityFromRequest(r).NodeID
	nodeID := r.PathValue("node_id")
	generation := r.PathValue("generation_id")
	var inventory NodeGenerationInventory
	var err error
	switch {
	case r.Method == http.MethodGet:
		inventory, err = s.store.NodeGenerations(r.Context(), identity, nodeID)
	case strings.HasSuffix(r.URL.Path, "/stage"):
		var request contract.NodeRegistration
		if err = decodeJSON(r, &request); err == nil {
			if request.NodeID != nodeID {
				err = protocolError(contract.ErrorInvalidRequest, "registration node_id must match path")
			} else {
				inventory, err = s.store.StageNodeGeneration(r.Context(), identity, request)
			}
		}
	case strings.HasSuffix(r.URL.Path, "/de-enroll"):
		var request DeEnrollNodeGenerationRequest
		if err = decodeJSON(r, &request); err == nil {
			inventory, err = s.store.DeEnrollNodeGeneration(r.Context(), identity, nodeID, request)
		}
	case strings.HasSuffix(r.URL.Path, "/promote"):
		var request PromoteNodeGenerationRequest
		if err = decodeJSON(r, &request); err == nil {
			inventory, err = s.store.PromoteNodeGeneration(r.Context(), identity, nodeID, request)
		}
	case strings.HasSuffix(r.URL.Path, "/heartbeat"):
		var request NodeGenerationHeartbeatRequest
		if err = decodeJSON(r, &request); err == nil {
			inventory, err = s.store.HeartbeatRetiringNodeGeneration(r.Context(), identity, nodeID, generation, request.BootSessionID)
		}
	case strings.HasSuffix(r.URL.Path, "/close"):
		var request CloseNodeGenerationRequest
		if err = decodeJSON(r, &request); err == nil {
			inventory, err = s.store.CloseNodeGeneration(r.Context(), identity, nodeID, generation, request)
		}
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, inventory)
}

func readPrimaryResidents(ctx context.Context, q queryer, nodeID, boot string, authority int64) ([]ResidentAttempt, error) {
	rows, err := q.QueryContext(ctx, `SELECT job_id,attempt_id,boot_session_id,authority_generation,fencing_token FROM attempts WHERE node_id=? AND boot_session_id=? AND authority_generation=? AND state IN ('claimed','running','awaiting-input') ORDER BY attempt_id`, nodeID, boot, authority)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	residents := []ResidentAttempt{}
	for rows.Next() {
		var r ResidentAttempt
		if err = rows.Scan(&r.JobID, &r.AttemptID, &r.BootSessionID, &r.AuthorityGeneration, &r.FencingToken); err != nil {
			return nil, err
		}
		residents = append(residents, r)
	}
	return residents, rows.Err()
}

// A session cannot withdraw protocol promises while resident work or another
// retiring generation still relies on the cohort's declaration.
func validateGenerationProtocolObservation(ctx context.Context, q queryer, nodeID string, capabilities map[string]bool) error {
	var previous []byte
	err := q.QueryRowContext(ctx, `SELECT protocols_json FROM node_generations WHERE node_id=? AND role='primary'`, nodeID).Scan(&previous)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	versions, err := retirementProtocols(capabilities)
	if err != nil {
		return err
	}
	var promised []uint32
	if err = json.Unmarshal(previous, &promised); err != nil {
		return err
	}
	var active bool
	if err = q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM attempts WHERE node_id=? AND state IN ('claimed','running','awaiting-input')) OR EXISTS(SELECT 1 FROM node_generations WHERE node_id=? AND role='retiring')`, nodeID, nodeID).Scan(&active); err != nil {
		return err
	}
	if active {
		for _, v := range promised {
			if !slices.Contains(versions, v) {
				return protocolError(contract.ErrorConflict, "live generation protocol promises cannot be withdrawn")
			}
		}
	}
	return nil
}

func refreshPrimaryGenerationObservation(ctx context.Context, tx *sql.Tx, nodeID string, now time.Time) error {
	var payload []byte
	err := tx.QueryRowContext(ctx, `SELECT registration_json FROM node_generations WHERE node_id=? AND role='primary'`, nodeID).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var registration contract.NodeRegistration
	if err = json.Unmarshal(payload, &registration); err != nil {
		return err
	}
	var capabilities, missing []byte
	var observed int64
	if err = tx.QueryRowContext(ctx, `SELECT capabilities_json,capability_revision,capability_observed_ns,missing_capabilities_json,capability_reason_code FROM nodes WHERE node_id=?`, nodeID).Scan(&capabilities, &registration.CapabilityRevision, &observed, &missing, &registration.CapabilityReasonCode); err != nil {
		return err
	}
	if err = json.Unmarshal(capabilities, &registration.Capabilities); err != nil {
		return err
	}
	if err = json.Unmarshal(missing, &registration.MissingCapabilities); err != nil {
		return err
	}
	registration.CapabilityObservedAt = time.Unix(0, observed).UTC()
	versions, err := retirementProtocols(registration.Capabilities)
	if err != nil {
		return err
	}
	payload, _ = json.Marshal(registration)
	protocols, _ := json.Marshal(versions)
	_, err = tx.ExecContext(ctx, `UPDATE node_generations SET registration_json=?,protocols_json=?,heartbeat_ns=? WHERE node_id=? AND role='primary'`, payload, protocols, now.UnixNano(), nodeID)
	return err
}

// NodeGenerationClosureReady exposes only L1's completion/Lease prerequisite.
// A true result does not prove local reap or evidence custody and does not close.
func (s *Store) NodeGenerationClosureReady(ctx context.Context, identity, nodeID, generation string) (bool, error) {
	tx, err := s.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if err = requireGenerationIdentity(ctx, tx, nodeID, identity); err != nil {
		return false, err
	}
	var role NodeGenerationRole
	if err = tx.QueryRowContext(ctx, `SELECT role FROM node_generations WHERE node_id=? AND generation_id=?`, nodeID, generation).Scan(&role); err != nil {
		return false, err
	}
	if role != NodeGenerationRetiring && role != NodeGenerationClosed {
		return false, protocolError(contract.ErrorConflict, "only retiring sessions can close")
	}
	ready, err := nodeGenerationClosureReady(ctx, tx, nodeID, generation, canonicalTime(s.clock.Now()))
	if err != nil {
		return false, err
	}
	return ready, tx.Commit()
}

func canonicalGenerationSystemBoot(value string) string {
	if value == "" {
		return ""
	}
	parts := strings.Split(value, ":")
	return parts[0] + ":" + parts[len(parts)-1]
}
