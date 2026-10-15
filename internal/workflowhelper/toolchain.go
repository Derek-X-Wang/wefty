package workflowhelper

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// typeScriptToolchain is what the TypeScript scaffold's package.json scripts
// run: esbuild bundles, tsc type checks against the Node types. The dogfood
// workflow pins more -- its lint stack, and Sandcastle as a run-time
// dependency -- and none of that belongs in a starter.
var typeScriptToolchain = []string{"@types/node", "esbuild", "typescript"}

// The scaffold's own package identity, shared by package.json.tmpl and the
// lockfile root so the two cannot disagree.
const (
	scaffoldPackageVersion = "0.1.0"
	scaffoldNodeEngine     = ">=22.7"
)

// pinnedPackage is one devDependency the scaffold pins to an exact version.
type pinnedPackage struct {
	Name    string
	Version string
}

// exactVersion is a version that names one published package: no range, tag,
// alias, path or URL. The scaffold promises pins, so anything else is refused.
var exactVersion = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$`)

// lockField is one key of a lockfile object with its value as npm wrote it.
type lockField struct {
	key   string
	value json.RawMessage
}

// lockEntry is one "packages" entry of a version 3 npm lockfile: its fields
// in the order npm wrote them, and the edges the derivation follows.
type lockEntry struct {
	fields []lockField

	Version              string            `json:"version"`
	Resolved             string            `json:"resolved"`
	Link                 bool              `json:"link"`
	Dependencies         map[string]string `json:"dependencies"`
	DevDependencies      map[string]string `json:"devDependencies"`
	OptionalDependencies map[string]string `json:"optionalDependencies"`
	PeerDependencies     map[string]string `json:"peerDependencies"`
	PeerDependenciesMeta map[string]struct {
		Optional bool `json:"optional"`
	} `json:"peerDependenciesMeta"`
}

type edgeKind int

const (
	edgeRequired edgeKind = iota
	edgeOptional
	edgePeer
)

type lockEdge struct {
	name string
	kind edgeKind
}

// edges are the dependencies npm installs for this package. An optional peer
// is not installed on its own account, and since it is both a peer and an
// optional edge it can never clear either flag, so it is left out entirely.
func (entry *lockEntry) edges() []lockEdge {
	var edges []lockEdge
	for _, name := range sortedKeys(entry.Dependencies) {
		edges = append(edges, lockEdge{name, edgeRequired})
	}
	for _, name := range sortedKeys(entry.OptionalDependencies) {
		edges = append(edges, lockEdge{name, edgeOptional})
	}
	for _, name := range sortedKeys(entry.PeerDependencies) {
		if !entry.PeerDependenciesMeta[name].Optional {
			edges = append(edges, lockEdge{name, edgePeer})
		}
	}
	return edges
}

// deriveTypeScriptToolchain takes the scaffold's pins and its whole
// package-lock.json from a dogfood lockfile, so `wefty workflow init --lang
// ts` installs exactly the build toolchain the dogfood workflow is proven
// with, and a Dependabot bump there needs no edit here (#952).
//
// Each tool is pinned to the version the dogfood lockfile resolves, and the
// scaffold's lockfile is the subset of the dogfood one that those tools pull
// in: every entry as npm wrote it, at the same location, with only the dev,
// optional and peer flags recomputed for a package that has nothing but these
// devDependencies.
func deriveTypeScriptToolchain(dogfoodLock []byte, packageName string, tools []string) ([]pinnedPackage, []byte, error) {
	locations, packages, err := readLockfile(dogfoodLock)
	if err != nil {
		return nil, nil, err
	}
	root, ok := packages[""]
	if !ok {
		return nil, nil, errors.New("the lockfile has no root package")
	}

	pins := make([]pinnedPackage, 0, len(tools))
	starts := make([]string, 0, len(tools))
	for _, tool := range tools {
		if _, ok := root.DevDependencies[tool]; !ok {
			return nil, nil, fmt.Errorf("%s is not one of the lockfile's devDependencies", tool)
		}
		location, ok := resolveLockLocation(packages, "", tool)
		if !ok {
			return nil, nil, fmt.Errorf("the lockfile does not install %s", tool)
		}
		entry := packages[location]
		if entry.Link || !exactVersion.MatchString(entry.Version) {
			return nil, nil, fmt.Errorf("the lockfile resolves %s to %q, which is not an exact published version", tool, entry.Version)
		}
		pins = append(pins, pinnedPackage{Name: tool, Version: entry.Version})
		starts = append(starts, location)
	}
	sort.Slice(pins, func(i, j int) bool { return pins[i].Name < pins[j].Name })

	// Every path from the scaffold's root starts with a dev edge. A package is
	// optional when no path avoids an optional edge, and a peer when no path
	// avoids a peer edge -- how npm itself computes these flags.
	installed, err := reachableLocations(packages, starts, func(edgeKind) bool { return true })
	if err != nil {
		return nil, nil, err
	}
	required, err := reachableLocations(packages, starts, func(kind edgeKind) bool { return kind != edgeOptional })
	if err != nil {
		return nil, nil, err
	}
	notPeer, err := reachableLocations(packages, starts, func(kind edgeKind) bool { return kind != edgePeer })
	if err != nil {
		return nil, nil, err
	}

	var out bytes.Buffer
	out.WriteString("{\n")
	fmt.Fprintf(&out, "  \"name\": %s,\n", jsonString(packageName))
	fmt.Fprintf(&out, "  \"version\": %s,\n", jsonString(scaffoldPackageVersion))
	out.WriteString("  \"lockfileVersion\": 3,\n  \"requires\": true,\n  \"packages\": {\n    \"\": {\n")
	fmt.Fprintf(&out, "      \"name\": %s,\n", jsonString(packageName))
	fmt.Fprintf(&out, "      \"version\": %s,\n", jsonString(scaffoldPackageVersion))
	out.WriteString("      \"devDependencies\": {\n")
	for index, pin := range pins {
		separator := ","
		if index == len(pins)-1 {
			separator = ""
		}
		fmt.Fprintf(&out, "        %s: %s%s\n", jsonString(pin.Name), jsonString(pin.Version), separator)
	}
	out.WriteString("      },\n      \"engines\": {\n")
	fmt.Fprintf(&out, "        \"node\": %s\n", jsonString(scaffoldNodeEngine))
	out.WriteString("      }\n    }")
	// The dogfood lockfile's own order, which is npm's.
	for _, location := range locations {
		if location.key == "" || !installed[location.key] {
			continue
		}
		fields := withLockFlags(packages[location.key].fields, !required[location.key], !notPeer[location.key])
		fmt.Fprintf(&out, ",\n    %s: {", jsonString(location.key))
		for index, field := range fields {
			if index > 0 {
				out.WriteString(",")
			}
			fmt.Fprintf(&out, "\n      %s: ", jsonString(field.key))
			if err := json.Indent(&out, field.value, "      ", "  "); err != nil {
				return nil, nil, fmt.Errorf("write the lockfile entry %q: %w", location.key, err)
			}
		}
		out.WriteString("\n    }")
	}
	out.WriteString("\n  }\n}\n")
	return pins, out.Bytes(), nil
}

// readLockfile reads a version 3 lockfile's packages: their locations in the
// order npm wrote them, and each entry by location.
func readLockfile(raw []byte) ([]lockField, map[string]*lockEntry, error) {
	var document struct {
		LockfileVersion int             `json:"lockfileVersion"`
		Packages        json.RawMessage `json:"packages"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, nil, fmt.Errorf("read the lockfile: %w", err)
	}
	if document.LockfileVersion != 3 {
		return nil, nil, fmt.Errorf("the lockfile is version %d; the scaffold is derived from a version 3 lockfile", document.LockfileVersion)
	}
	locations, err := orderedFields(document.Packages)
	if err != nil {
		return nil, nil, fmt.Errorf("read the lockfile's packages: %w", err)
	}
	packages := make(map[string]*lockEntry, len(locations))
	for _, location := range locations {
		entry := &lockEntry{}
		if err := json.Unmarshal(location.value, entry); err != nil {
			return nil, nil, fmt.Errorf("read the lockfile entry %q: %w", location.key, err)
		}
		if entry.fields, err = orderedFields(location.value); err != nil {
			return nil, nil, fmt.Errorf("read the lockfile entry %q: %w", location.key, err)
		}
		packages[location.key] = entry
	}
	return locations, packages, nil
}

// reachableLocations walks the lockfile from starts along the edges follow
// admits. A required or peer dependency the lockfile cannot resolve is an
// inconsistent lockfile; an optional one may be absent (npm skips it).
func reachableLocations(packages map[string]*lockEntry, starts []string, follow func(edgeKind) bool) (map[string]bool, error) {
	reached := make(map[string]bool)
	queue := append([]string(nil), starts...)
	for _, start := range starts {
		reached[start] = true
	}
	for len(queue) > 0 {
		location := queue[0]
		queue = queue[1:]
		entry := packages[location]
		if entry.Link {
			return nil, fmt.Errorf("the lockfile links %q to a local directory; the scaffold can only pin published packages", location)
		}
		// A file: or relative tarball resolves only beside the dogfood
		// workflow, so npm ci in the scaffold could not find it.
		if entry.Resolved != "" && !strings.HasPrefix(entry.Resolved, "https://") {
			return nil, fmt.Errorf("the lockfile resolves %q from %q; the scaffold can only pin packages fetched from a registry", location, entry.Resolved)
		}
		for _, edge := range entry.edges() {
			if !follow(edge.kind) {
				continue
			}
			target, ok := resolveLockLocation(packages, location, edge.name)
			if !ok {
				if edge.kind == edgeOptional {
					continue
				}
				return nil, fmt.Errorf("the lockfile does not install %s, which %q depends on", edge.name, location)
			}
			if !reached[target] {
				reached[target] = true
				queue = append(queue, target)
			}
		}
	}
	return reached, nil
}

// resolveLockLocation finds the package a dependency of the package at from
// loads, the way Node does: from's own node_modules first, then each
// enclosing node_modules up to the root's.
func resolveLockLocation(packages map[string]*lockEntry, from, name string) (string, bool) {
	directory := from
	for {
		location := "node_modules/" + name
		if directory != "" {
			location = directory + "/" + location
		}
		if _, ok := packages[location]; ok {
			return location, true
		}
		if directory == "" {
			return "", false
		}
		if cut := strings.LastIndex(directory, "/node_modules/"); cut >= 0 {
			directory = directory[:cut]
		} else {
			directory = ""
		}
	}
}

// lockFlagKeys are the flags npm derives from the dependency graph rather than
// from the package, so they are recomputed for the scaffold's smaller graph.
var lockFlagKeys = []string{"dev", "optional", "devOptional", "peer"}

// withLockFlags gives an entry the flags it has in the scaffold: always dev,
// since the scaffold has nothing but devDependencies, plus optional and peer
// where the scaffold's graph says so. An entry whose flags already agree is
// returned as npm wrote it.
func withLockFlags(fields []lockField, optional, peer bool) []lockField {
	want := map[string]bool{"dev": true, "optional": optional, "peer": peer}
	agrees := true
	for _, flag := range lockFlagKeys {
		has := slices.ContainsFunc(fields, func(field lockField) bool { return field.key == flag })
		if has != want[flag] {
			agrees = false
		}
	}
	if agrees {
		return fields
	}
	kept := slices.DeleteFunc(slices.Clone(fields), func(field lockField) bool {
		return slices.Contains(lockFlagKeys, field.key)
	})
	for _, flag := range lockFlagKeys {
		if !want[flag] {
			continue
		}
		added := lockField{key: flag, value: json.RawMessage("true")}
		at := len(kept)
		for index, field := range kept {
			if npmKeyBefore(added, field) {
				at = index
				break
			}
		}
		kept = slices.Insert(kept, at, added)
	}
	return kept
}

// npmPreferredKeys and npmKeyBefore are the key order npm writes a lockfile
// in (json-stringify-nice): scalars and arrays before objects, these keys
// first, then the rest alphabetically. It only places a flag npm did not
// write; every other key keeps the place npm gave it.
var npmPreferredKeys = []string{"name", "version", "lockfileVersion", "resolved", "integrity", "requires", "packages", "dependencies"}

func npmKeyBefore(a, b lockField) bool {
	aObject, bObject := isJSONObject(a.value), isJSONObject(b.value)
	if aObject != bObject {
		return bObject
	}
	aPreferred, bPreferred := slices.Index(npmPreferredKeys, a.key), slices.Index(npmPreferredKeys, b.key)
	switch {
	case aPreferred >= 0 && bPreferred >= 0:
		return aPreferred < bPreferred
	case aPreferred >= 0 || bPreferred >= 0:
		return aPreferred >= 0
	}
	return strings.ToLower(a.key) < strings.ToLower(b.key)
}

func isJSONObject(value json.RawMessage) bool {
	trimmed := bytes.TrimSpace(value)
	return len(trimmed) > 0 && trimmed[0] == '{'
}

// orderedFields reads a JSON object's keys in document order, which
// encoding/json's maps do not keep.
func orderedFields(raw []byte) ([]lockField, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if token, err := decoder.Token(); err != nil {
		return nil, err
	} else if delimiter, ok := token.(json.Delim); !ok || delimiter != '{' {
		return nil, errors.New("not a JSON object")
	}
	var fields []lockField
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected %v where a key belongs", token)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		fields = append(fields, lockField{key: key, value: value})
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	return fields, nil
}

// jsonString encodes a string the way npm does, without encoding/json's HTML
// escapes (it would write ">=22.7" as ">=22.7").
func jsonString(value string) string {
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
	return strings.TrimSuffix(out.String(), "\n")
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
