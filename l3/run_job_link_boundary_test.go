package l3

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"unicode"
)

// Protected storage (not public protocol vocabulary): runs.l1_job_id,
// job_link_settled, job_link_failures, job_link_retry_ns, dispatch_attempt_ns;
// dispatch_outbox.job_id, dispatched_ns, last_error, token_delivery; existence,
// insertion and deletion of run_cancellations. Delivery status is separate.
var linkProtectedColumns = map[string]map[string]bool{
	"runs":              {"l1_job_id": true, "job_link_settled": true, "job_link_failures": true, "job_link_retry_ns": true, "dispatch_attempt_ns": true},
	"dispatch_outbox":   {"job_id": true, "dispatched_ns": true, "last_error": true, "token_delivery": true},
	"run_cancellations": {},
}

type linkBoundaryEntry struct {
	File        string `json:"file"`
	Function    string `json:"function"`
	Fingerprint string `json:"fingerprint"`
	Count       int    `json:"count"`
	Reason      string `json:"reason"`
	Retirement  string `json:"retirement"`
}

type linkSQLPackage struct {
	files     map[string]*ast.File
	imports   map[*ast.File]map[string]string
	constants map[string]ast.Expr
	root      string
	packages  map[string]*linkSQLPackage
}

func parseLinkSQLPackage(root string, sources map[string]string) (*linkSQLPackage, error) {
	p := &linkSQLPackage{files: map[string]*ast.File{}, imports: map[*ast.File]map[string]string{}, constants: map[string]ast.Expr{}, root: root, packages: map[string]*linkSQLPackage{}}
	for name, source := range sources {
		f, err := parser.ParseFile(token.NewFileSet(), name, source, 0)
		if err != nil {
			return nil, err
		}
		p.files[name] = f
		aliases := map[string]string{}
		for _, spec := range f.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return nil, err
			}
			alias := filepath.Base(path)
			if spec.Name != nil {
				alias = spec.Name.Name
			}
			aliases[alias] = path
		}
		p.imports[f] = aliases
		for _, decl := range f.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				v := spec.(*ast.ValueSpec)
				for i, n := range v.Names {
					if i < len(v.Values) {
						p.constants[n.Name] = v.Values[i]
					}
				}
			}
		}
	}
	return p, nil
}

// Only immutable, completely resolved expressions are trusted. Variables,
// assembly, helper results and builders stay opaque and fail closed. Their
// entire owner body enters the fingerprint, so legacy opaque sites cannot
// quietly change their assembly while keeping the execution call unchanged.
func (p *linkSQLPackage) constant(file *ast.File, expr ast.Expr, seen map[string]bool) (string, bool) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind == token.STRING {
			value, err := strconv.Unquote(e.Value)
			return value, err == nil
		}
	case *ast.ParenExpr:
		return p.constant(file, e.X, seen)
	case *ast.BinaryExpr:
		if e.Op == token.ADD {
			left, a := p.constant(file, e.X, seen)
			right, b := p.constant(file, e.Y, seen)
			return left + right, a && b
		}
	case *ast.Ident:
		if e.Obj != nil {
			if e.Obj.Kind != ast.Con {
				return "", false
			}
			if declaration, ok := e.Obj.Decl.(*ast.ValueSpec); ok {
				for i, name := range declaration.Names {
					if name.Name == e.Name && i < len(declaration.Values) {
						if seen[e.Name] {
							return "", false
						}
						next := map[string]bool{}
						for k, v := range seen {
							next[k] = v
						}
						next[e.Name] = true
						return p.constant(file, declaration.Values[i], next)
					}
				}
			}
		}
		if seen[e.Name] {
			return "", false
		}
		value, ok := p.constants[e.Name]
		if !ok {
			return "", false
		}
		next := map[string]bool{}
		for k, v := range seen {
			next[k] = v
		}
		next[e.Name] = true
		// A constant may use an import declared in a different file.
		for _, owner := range p.files {
			found := false
			ast.Inspect(owner, func(n ast.Node) bool {
				if n == value {
					found = true
				}
				return !found
			})
			if found {
				return p.constant(owner, value, next)
			}
		}
	case *ast.SelectorExpr:
		alias, ok := e.X.(*ast.Ident)
		if !ok {
			return "", false
		}
		if alias.Obj != nil {
			return "", false
		}
		path := p.imports[file][alias.Name]
		const prefix = "github.com/Derek-X-Wang/wefty/"
		if !strings.HasPrefix(path, prefix) || p.root == "" {
			return "", false
		}
		dependency := p.packages[path]
		if dependency == nil {
			dir := filepath.Join(p.root, strings.TrimPrefix(path, prefix))
			entries, err := os.ReadDir(dir)
			if err != nil {
				return "", false
			}
			sources := map[string]string{}
			for _, entry := range entries {
				if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
					continue
				}
				body, err := os.ReadFile(filepath.Join(dir, entry.Name()))
				if err != nil {
					return "", false
				}
				sources[entry.Name()] = string(body)
			}
			var errParse error
			dependency, errParse = parseLinkSQLPackage(p.root, sources)
			if errParse != nil {
				return "", false
			}
			dependency.packages = p.packages
			p.packages[path] = dependency
		}
		return dependency.constant(nil, ast.NewIdent(e.Sel.Name), map[string]bool{})
	}
	return "", false
}

func linkGoFingerprint(node ast.Node) string {
	if field, ok := node.(*ast.Field); ok {
		names := ""
		for _, name := range field.Names {
			names += name.Name + ","
		}
		return names + linkGoFingerprint(field.Type)
	}
	var body bytes.Buffer
	if err := format.Node(&body, token.NewFileSet(), node); err != nil {
		panic(err)
	}
	return body.String()
}

// SQL lexing distinguishes quoted identifiers from values and drops comments.
// Table/alias collection includes nested queries, not just the outer FROM.
func linkSQLTokens(sql string) ([]string, bool) {
	var tokens []string
	for i := 0; i < len(sql); {
		c := sql[i]
		if unicode.IsSpace(rune(c)) {
			i++
			continue
		}
		if c == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			continue
		}
		if c == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			end := strings.Index(sql[i+2:], "*/")
			if end < 0 {
				return nil, false
			}
			i += end + 4
			continue
		}
		if c == '\'' {
			i++
			var value strings.Builder
			closed := false
			for i < len(sql) {
				if sql[i] == '\'' {
					i++
					if i < len(sql) && sql[i] == '\'' {
						value.WriteByte('\'')
						i++
						continue
					}
					closed = true
					break
				}
				value.WriteByte(sql[i])
				i++
			}
			if !closed {
				return nil, false
			}
			tokens = append(tokens, "<value>:"+strings.ToLower(value.String()))
			continue
		}
		if c == '"' || c == '`' || c == '[' {
			close := c
			if c == '[' {
				close = ']'
			}
			i++
			var value strings.Builder
			closed := false
			for i < len(sql) {
				if sql[i] == close {
					i++
					if i < len(sql) && sql[i] == close {
						value.WriteByte(close)
						i++
						continue
					}
					closed = true
					break
				}
				value.WriteByte(sql[i])
				i++
			}
			if !closed {
				return nil, false
			}
			tokens = append(tokens, strings.ToLower(value.String()))
			continue
		}
		if unicode.IsLetter(rune(c)) || c == '_' {
			start := i
			for i < len(sql) && (unicode.IsLetter(rune(sql[i])) || unicode.IsDigit(rune(sql[i])) || sql[i] == '_') {
				i++
			}
			tokens = append(tokens, strings.ToLower(sql[start:i]))
			continue
		}
		tokens = append(tokens, string(c))
		i++
	}
	return tokens, true
}

// SQLite accepts single-quoted names where the grammar requires an identifier.
// https://www.sqlite.org/lang_keywords.html documents this compatibility rule.
// In value positions they remain values; only table/alias and qualified-column
// positions unwrap them.
func linkSQLIdentifier(token string) string { return strings.TrimPrefix(token, "<value>:") }

func linkSQLProtected(sql string) bool {
	tokens, ok := linkSQLTokens(sql)
	if !ok {
		return true
	}
	tables := map[string]bool{}
	aliases := map[string]map[string]bool{}
	add := func(alias, table string) {
		if aliases[alias] == nil {
			aliases[alias] = map[string]bool{}
		}
		aliases[alias][table] = true
	}
	for i, t := range tokens {
		if linkProtectedColumns[t] != nil {
			tables[t] = true
			add(t, t)
		}
		j := i + 1
		if t != "from" && t != "join" && t != "update" && t != "into" && t != "table" {
			if linkProtectedColumns[t] == nil {
				continue
			}
			j = i
		}
		if t == "table" {
			for j < len(tokens) && (tokens[j] == "if" || tokens[j] == "not" || tokens[j] == "exists") {
				j++
			}
		}
		if j >= len(tokens) || tokens[j] == "(" {
			continue
		}
		table := linkSQLIdentifier(tokens[j])
		if j+2 < len(tokens) && tokens[j+1] == "." {
			j += 2
			table = linkSQLIdentifier(tokens[j])
		}
		if linkProtectedColumns[table] == nil {
			continue
		}
		tables[table] = true
		add(table, table)
		j++
		if j < len(tokens) && tokens[j] == "as" {
			j++
		}
		if j < len(tokens) && tokens[j] != "(" && tokens[j] != "where" && tokens[j] != "set" && tokens[j] != "on" && tokens[j] != "join" && tokens[j] != "order" && tokens[j] != "group" && tokens[j] != "limit" && tokens[j] != "left" && tokens[j] != "cross" && tokens[j] != "inner" && tokens[j] != ";" && tokens[j] != "," {
			add(linkSQLIdentifier(tokens[j]), table)
		}
	}
	// Replacement can delete the existing row, including omitted protected
	// columns. Positional insertion implicitly writes every column.
	for i, t := range tokens {
		if t != "insert" && t != "replace" {
			continue
		}
		replacement := t == "replace"
		j := i + 1
		if t == "insert" {
			for j < len(tokens) && tokens[j] != "into" {
				if tokens[j] == "replace" {
					replacement = true
				}
				j++
			}
		}
		if j < len(tokens) && tokens[j] == "into" {
			j++
		}
		if j >= len(tokens) {
			continue
		}
		table := linkSQLIdentifier(tokens[j])
		if j+2 < len(tokens) && tokens[j+1] == "." {
			j += 2
			table = linkSQLIdentifier(tokens[j])
		}
		if linkProtectedColumns[table] != nil && (replacement || j+1 >= len(tokens) || tokens[j+1] != "(") {
			return true
		}
	}
	// A Run-filtered or Run-joined cancellation read reveals ownership even
	// when it projects only a literal, cardinality or a delivery column.
	// Known delivery-only sites are individually recorded in the exact ratchet.
	if tables["run_cancellations"] {
		selecting, runScoped := false, tables["runs"]
		for _, t := range tokens {
			selecting = selecting || t == "select"
			runScoped = runScoped || linkSQLIdentifier(t) == "run_id"
		}
		if selecting && runScoped {
			return true
		}
	}
	for i, t := range tokens {
		if t == "*" {
			// COUNT(*) reads row cardinality, not every protected column. For a
			// cancellation, cardinality is itself ownership knowledge.
			if i >= 2 && tokens[i-1] == "(" && tokens[i-2] == "count" {
				if tables["run_cancellations"] {
					return true
				}
				continue
			}
			if i >= 2 && tokens[i-1] == "." {
				for table := range aliases[linkSQLIdentifier(tokens[i-2])] {
					if tables[table] {
						return true
					}
				}
				continue
			}
			if len(tables) > 0 {
				return true
			}
		}
		if i >= 2 && tokens[i-1] == "." {
			for table := range aliases[linkSQLIdentifier(tokens[i-2])] {
				if linkProtectedColumns[table][linkSQLIdentifier(t)] {
					return true
				}
			}
			continue
		}
		for table := range tables {
			if linkProtectedColumns[table][t] {
				return true
			}
		}
	}
	if tables["run_cancellations"] {
		selecting := false
		assigning := false
		for _, t := range tokens {
			if t == "select" {
				selecting = true
				continue
			}
			if t == "from" {
				selecting = false
			}
			if t == "set" {
				assigning = true
			}
			if t == "where" {
				assigning = false
			}
			if (selecting || assigning) && t == "run_id" {
				return true
			}
		}
		// INSERT/DELETE/DROP and EXISTS or SELECT 1 arbitrate ownership even
		// without selecting the ownership key. Delivery-only UPDATE/SELECT stays free.
		for _, t := range tokens {
			if t == "insert" || t == "replace" || t == "delete" || t == "drop" || t == "create" || t == "alter" || t == "exists" {
				return true
			}
		}
		for i, t := range tokens {
			if t == "select" && i+1 < len(tokens) && tokens[i+1] == "1" {
				return true
			}
		}
	}
	return false
}

// sqlParameterIndex follows named wrappers and method values as well as direct
// SQL handles. Merely assigning db to another variable cannot hide the site.
func linkSQLMethod(name string) (int, bool) {
	switch name {
	case "Exec", "Query", "QueryRow", "Prepare":
		return 0, true
	case "ExecContext", "QueryContext", "QueryRowContext", "PrepareContext":
		return 1, true
	}
	return 0, false
}

func linkCallName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return e.Sel.Name
	}
	return ""
}

func linkFunctionParameters(fn *ast.FuncDecl) map[string]int {
	params := map[string]int{}
	i := 0
	if fn.Type.Params != nil {
		for _, field := range fn.Type.Params.List {
			if len(field.Names) == 0 {
				i++
				continue
			}
			for _, name := range field.Names {
				params[name.Name] = i
				i++
			}
		}
	}
	return params
}

func (p *linkSQLPackage) sites() []linkBoundaryEntry {
	// A wrapper whose parameter reaches SQL gains the same guarded argument
	// at every caller. Iterate to close chains of forwarding wrappers.
	targets := map[string]map[int]bool{}
	opaqueTargets := map[string]bool{}
	for _, name := range []string{"Exec", "Query", "QueryRow", "Prepare", "ExecContext", "QueryContext", "QueryRowContext", "PrepareContext"} {
		index, _ := linkSQLMethod(name)
		targets[name] = map[int]bool{index: true}
	}
	for changed := true; changed; {
		changed = false
		for _, file := range p.files {
			for _, decl := range file.Decls {
				gen, ok := decl.(*ast.GenDecl)
				if !ok || gen.Tok != token.VAR {
					continue
				}
				aliases, opaque := linkLocalSQLTargets(gen, targets, opaqueTargets)
				for name, indexes := range aliases {
					if targets[name] == nil {
						targets[name] = map[int]bool{}
					}
					for index := range indexes {
						if !targets[name][index] {
							targets[name][index] = true
							changed = true
						}
					}
					if opaque[name] && !opaqueTargets[name] {
						opaqueTargets[name] = true
						changed = true
					}
				}
			}
		}
		for _, file := range p.files {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok {
					continue
				}
				params := linkFunctionParameters(fn)
				localTargets, localOpaque := linkLocalSQLTargets(fn, targets, opaqueTargets)
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					callName := linkCallName(call.Fun)
					indexes := targets[callName]
					if indexes == nil {
						indexes = localTargets[callName]
					}
					for index := range indexes {
						if index >= len(call.Args) {
							continue
						}
						for _, ident := range linkSQLDependencies(call.Args[index]) {
							parameter, found := params[ident.Name]
							if !found {
								continue
							}
							if targets[fn.Name.Name] == nil {
								targets[fn.Name.Name] = map[int]bool{}
							}
							if !targets[fn.Name.Name][parameter] {
								targets[fn.Name.Name][parameter] = true
								changed = true
							}
							pure, ok := linkForwardedSQLParameter(call.Args[index], params, map[*ast.Object]bool{})
							if (!ok || pure != parameter || (opaqueTargets[callName] || localOpaque[callName])) && !opaqueTargets[fn.Name.Name] {
								opaqueTargets[fn.Name.Name] = true
								changed = true
							}
							continue
						}
					}
					return true
				})
			}
		}
	}
	sites := map[string]linkBoundaryEntry{}
	for name, file := range p.files {
		if name == "run_job_link.go" || name == "run_job_link_builder_test.go" {
			continue
		}
		inspect := func(owner string, node ast.Node) {
			// Bind method values in this owner; reassignment conservatively keeps the
			// SQL target so it cannot erase the boundary.
			localTargets, localOpaque := linkLocalSQLTargets(node, targets, opaqueTargets)
			ast.Inspect(node, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if p.nonSQLURLQuery(file, call) {
					return true
				}
				callName := linkCallName(call.Fun)
				indexes := targets[callName]
				if indexes == nil {
					indexes = localTargets[callName]
				}
				dynamic := p.opaqueSQLCall(file, call)
				if len(dynamic) > 0 {
					merged := map[int]bool{}
					for index := range indexes {
						merged[index] = true
					}
					for index := range dynamic {
						merged[index] = true
					}
					indexes = merged
				}
				for index := range indexes {
					var argument ast.Expr
					if index < len(call.Args) {
						argument = call.Args[index]
					}
					sql, resolved := p.constant(file, argument, map[string]bool{})
					// Inputs to an SQL constructor are fragments, not complete
					// statements. Individually safe fragments cannot authorize
					// assembled identifiers through a grandfathered helper.
					opaque := opaqueTargets[callName] || localOpaque[callName] || len(dynamic) > 0
					if opaque {
						sql = ""
						resolved = false
						arguments := append([]ast.Expr(nil), call.Args...)
						if len(dynamic) > 0 {
							arguments = append(arguments, call.Fun)
						}
						argument = &ast.CompositeLit{Elts: arguments}
					}
					if resolved && !linkSQLProtected(sql) {
						continue
					}
					fingerprintInput := linkGoFingerprint(call) + "\x00" + sql
					if !resolved {
						fingerprintInput += "\x00opaque SQL\x00" + linkGoFingerprint(node)
						var declarations []string
						seen := map[ast.Node]bool{}
						for _, id := range linkSQLDependencies(argument) {
							if id.Obj != nil {
								if declaration, ok := id.Obj.Decl.(ast.Node); ok && !seen[declaration] {
									declarations = append(declarations, linkGoFingerprint(declaration))
									seen[declaration] = true
								}
							} else {
								// Package declarations referenced across files do not
								// share parser objects. Resolve their names explicitly.
								for _, source := range p.files {
									for _, decl := range source.Decls {
										if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == id.Name && !seen[fn] {
											declarations = append(declarations, linkGoFingerprint(fn))
											seen[fn] = true
										}
										if gen, ok := decl.(*ast.GenDecl); ok {
											for _, spec := range gen.Specs {
												if value, ok := spec.(*ast.ValueSpec); ok {
													for _, name := range value.Names {
														if name.Name == id.Name && !seen[value] {
															declarations = append(declarations, linkGoFingerprint(value))
															seen[value] = true
														}
													}
												}
											}
										}
									}
								}
							}
						}
						sort.Strings(declarations)
						fingerprintInput += strings.Join(declarations, "\x00")
					}
					fingerprint := fmt.Sprintf("%x", sha256.Sum256([]byte(fingerprintInput)))
					key := name + ":" + owner + ":" + fingerprint
					site := sites[key]
					site.File = name
					site.Function = owner
					site.Fingerprint = fingerprint
					site.Count++
					sites[key] = site
					if opaque {
						break
					}
				}
				return true
			})
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok {
				owner := fn.Name.Name
				if fn.Recv != nil {
					owner = linkGoFingerprint(fn.Recv.List[0].Type) + "." + owner
				}
				inspect(owner, fn)
				continue
			}
			if gen, ok := decl.(*ast.GenDecl); ok && gen.Tok == token.VAR {
				for _, spec := range gen.Specs {
					value := spec.(*ast.ValueSpec)
					owner := "var"
					for _, id := range value.Names {
						owner += " " + id.Name
					}
					inspect(owner, value)
				}
			}
		}
	}
	result := make([]linkBoundaryEntry, 0, len(sites))
	for _, site := range sites {
		result = append(result, site)
	}
	sort.Slice(result, func(i, j int) bool {
		a, b := result[i], result[j]
		if a.File != b.File {
			return a.File < b.File
		}
		if a.Function != b.Function {
			return a.Function < b.Function
		}
		return a.Fingerprint < b.Fingerprint
	})
	return result
}

func checkLinkBoundaryRatchet(actual, allowed []linkBoundaryEntry) error {
	seen := map[string]bool{}
	for _, entry := range allowed {
		key := entry.File + ":" + entry.Function + ":" + entry.Fingerprint
		if seen[key] || entry.Count < 1 || entry.Reason == "" || !strings.HasPrefix(entry.Retirement, "#") {
			return fmt.Errorf("invalid or duplicate ratchet entry: %+v", entry)
		}
		ticket, err := strconv.Atoi(strings.TrimPrefix(entry.Retirement, "#"))
		if err != nil || ticket < 779 || ticket > 785 {
			return fmt.Errorf("invalid retirement: %+v", entry)
		}
		seen[key] = true
	}
	expected := append([]linkBoundaryEntry(nil), allowed...)
	for i := range expected {
		expected[i].Reason = ""
		expected[i].Retirement = ""
	}
	sort.Slice(expected, func(i, j int) bool {
		a, b := expected[i], expected[j]
		if a.File != b.File {
			return a.File < b.File
		}
		if a.Function != b.Function {
			return a.Function < b.Function
		}
		return a.Fingerprint < b.Fingerprint
	})
	if !reflect.DeepEqual(actual, expected) {
		body, _ := json.MarshalIndent(actual, "", "  ")
		return fmt.Errorf("Run job link boundary changed (unexpected, changed, duplicate or obsolete sites); actual:\n%s", body)
	}
	return nil
}

func TestRunJobLinkStorageBoundary(t *testing.T) {
	sources := map[string]string{}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		body, err := os.ReadFile(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		sources[entry.Name()] = string(body)
	}
	p, err := parseLinkSQLPackage("..", sources)
	if err != nil {
		t.Fatal(err)
	}
	actual := p.sites()
	body, err := os.ReadFile("testdata/run_job_link_boundary.json")
	if err != nil {
		t.Fatal(err)
	}
	var allowed []linkBoundaryEntry
	if err := json.Unmarshal(body, &allowed); err != nil {
		t.Fatal(err)
	}
	if err := checkLinkBoundaryRatchet(actual, allowed); err != nil {
		t.Fatal(err)
	}
}

// Parser object identity follows local aliases without conflating shadowed
// names. The walk is conservative: all assignments inside a declaration enter
// opaque fingerprints, and a parameter hidden behind an alias still taints the
// wrapper's callers.
func linkSQLDependencies(expr ast.Expr) []*ast.Ident {
	var result []*ast.Ident
	visited := map[ast.Node]bool{}
	var walk func(ast.Node)
	walk = func(node ast.Node) {
		if node == nil || visited[node] {
			return
		}
		visited[node] = true
		ast.Inspect(node, func(n ast.Node) bool {
			id, ok := n.(*ast.Ident)
			if !ok {
				return true
			}
			result = append(result, id)
			if id.Obj != nil {
				if declaration, ok := id.Obj.Decl.(ast.Node); ok {
					walk(declaration)
				}
			}
			return true
		})
	}
	walk(expr)
	return result
}

func linkForwardedSQLParameter(expr ast.Expr, params map[string]int, seen map[*ast.Object]bool) (int, bool) {
	id, ok := expr.(*ast.Ident)
	if !ok {
		return 0, false
	}
	if index, ok := params[id.Name]; ok {
		return index, true
	}
	if id.Obj == nil || seen[id.Obj] {
		return 0, false
	}
	seen[id.Obj] = true
	switch declaration := id.Obj.Decl.(type) {
	case *ast.ValueSpec:
		for i, name := range declaration.Names {
			if name.Name == id.Name && i < len(declaration.Values) {
				return linkForwardedSQLParameter(declaration.Values[i], params, seen)
			}
		}
	case *ast.AssignStmt:
		for i, left := range declaration.Lhs {
			if name, ok := left.(*ast.Ident); ok && name.Name == id.Name && i < len(declaration.Rhs) {
				return linkForwardedSQLParameter(declaration.Rhs[i], params, seen)
			}
		}
	}
	return 0, false
}

func linkLocalSQLTargets(node ast.Node, targets map[string]map[int]bool, opaqueTargets map[string]bool) (map[string]map[int]bool, map[string]bool) {
	local := map[string]map[int]bool{}
	opaque := map[string]bool{}
	for changed := true; changed; {
		changed = false
		ast.Inspect(node, func(n ast.Node) bool {
			var left, right []ast.Expr
			switch binding := n.(type) {
			case *ast.AssignStmt:
				left = binding.Lhs
				right = binding.Rhs
			case *ast.ValueSpec:
				for _, name := range binding.Names {
					left = append(left, name)
				}
				right = binding.Values
			default:
				return true
			}
			for i, value := range right {
				if i >= len(left) {
					continue
				}
				id, ok := left[i].(*ast.Ident)
				if !ok {
					continue
				}
				sourceName := linkCallName(value)
				indexes := targets[sourceName]
				if indexes == nil {
					indexes = local[linkCallName(value)]
				}
				if indexes == nil {
					continue
				}
				if (opaqueTargets[sourceName] || opaque[sourceName]) && !opaque[id.Name] {
					opaque[id.Name] = true
					changed = true
				}
				if local[id.Name] == nil {
					local[id.Name] = map[int]bool{}
					changed = true
				}
				for k := range indexes {
					if !local[id.Name][k] {
						local[id.Name][k] = true
						changed = true
					}
				}
			}
			return true
		})
	}
	return local, opaque
}

// Query is also a net/url method. Exempt that proven standard-library type,
// rather than exempting receiver names such as URL (which SQL handles can use).
func (p *linkSQLPackage) nonSQLURLQuery(file *ast.File, call *ast.CallExpr) bool {
	method, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || method.Sel.Name != "Query" {
		return false
	}
	isType := func(expr ast.Expr, path, name string) bool {
		id, ok := expr.(*ast.Ident)
		if !ok || id.Obj == nil {
			return false
		}
		field, ok := id.Obj.Decl.(*ast.Field)
		if !ok {
			return false
		}
		typ := field.Type
		if pointer, ok := typ.(*ast.StarExpr); ok {
			typ = pointer.X
		}
		selector, ok := typ.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		alias, ok := selector.X.(*ast.Ident)
		return ok && p.imports[file][alias.Name] == path && selector.Sel.Name == name
	}
	if isType(method.X, "net/url", "URL") {
		return true
	}
	member, ok := method.X.(*ast.SelectorExpr)
	return ok && member.Sel.Name == "URL" && isType(member.X, "net/http", "Request")
}

// expressionType resolves local function values and package-declared types
// without depending on successful type checking of corrupt/bypass fixtures.
// SQL handles need not keep an SQL method name when passed through a callback.
func (p *linkSQLPackage) expressionType(expr ast.Expr, seen map[ast.Node]bool) ast.Expr {
	if expr == nil || seen[expr] {
		return nil
	}
	seen[expr] = true
	switch e := expr.(type) {
	case *ast.ParenExpr:
		return p.expressionType(e.X, seen)
	case *ast.BasicLit:
		if e.Kind == token.STRING {
			return ast.NewIdent("string")
		}
	case *ast.BinaryExpr:
		if e.Op == token.ADD {
			return p.expressionType(e.X, seen)
		}
	case *ast.FuncLit:
		return e.Type
	case *ast.CompositeLit:
		return e.Type
	case *ast.Ident:
		if e.Obj != nil {
			switch d := e.Obj.Decl.(type) {
			case *ast.Field:
				return d.Type
			case *ast.FuncDecl:
				return d.Type
			case *ast.ValueSpec:
				if d.Type != nil {
					return d.Type
				}
				for i, n := range d.Names {
					if n.Name == e.Name && i < len(d.Values) {
						return p.expressionType(d.Values[i], seen)
					}
				}
			case *ast.AssignStmt:
				for i, left := range d.Lhs {
					if id, ok := left.(*ast.Ident); ok && id.Name == e.Name && i < len(d.Rhs) {
						return p.expressionType(d.Rhs[i], seen)
					}
				}
			}
		}
		for _, file := range p.files {
			for _, decl := range file.Decls {
				if d, ok := decl.(*ast.FuncDecl); ok && d.Recv == nil && d.Name.Name == e.Name {
					return d.Type
				}
				if d, ok := decl.(*ast.GenDecl); ok {
					for _, spec := range d.Specs {
						if v, ok := spec.(*ast.ValueSpec); ok {
							for i, n := range v.Names {
								if n.Name == e.Name {
									if v.Type != nil {
										return v.Type
									}
									if i < len(v.Values) {
										return p.expressionType(v.Values[i], seen)
									}
								}
							}
						}
					}
				}
			}
		}
	case *ast.SelectorExpr:
		typ := p.underlyingType(p.expressionType(e.X, seen), map[string]bool{})
		if ptr, ok := typ.(*ast.StarExpr); ok {
			typ = p.underlyingType(ptr.X, map[string]bool{})
		}
		if st, ok := typ.(*ast.StructType); ok {
			for _, field := range st.Fields.List {
				for _, n := range field.Names {
					if n.Name == e.Sel.Name {
						return field.Type
					}
				}
			}
		}
		if method := p.methodDeclaration(e); method != nil {
			return method.Type
		}
	case *ast.CallExpr:
		if fn, ok := p.underlyingType(p.expressionType(e.Fun, seen), map[string]bool{}).(*ast.FuncType); ok && fn.Results != nil && len(fn.Results.List) == 1 {
			return fn.Results.List[0].Type
		}
	case *ast.IndexExpr:
		if a, ok := p.underlyingType(p.expressionType(e.X, seen), map[string]bool{}).(*ast.ArrayType); ok {
			return a.Elt
		}
	}
	return nil
}

func (p *linkSQLPackage) underlyingType(expr ast.Expr, seen map[string]bool) ast.Expr {
	id, ok := expr.(*ast.Ident)
	if !ok || seen[id.Name] {
		return expr
	}
	seen[id.Name] = true
	for _, file := range p.files {
		for _, decl := range file.Decls {
			if gen, ok := decl.(*ast.GenDecl); ok && gen.Tok == token.TYPE {
				for _, spec := range gen.Specs {
					d := spec.(*ast.TypeSpec)
					if d.Name.Name == id.Name {
						return p.underlyingType(d.Type, seen)
					}
				}
			}
		}
	}
	return expr
}

func (p *linkSQLPackage) opaqueSQLCall(file *ast.File, call *ast.CallExpr) map[int]bool {
	// Ordinary declared functions are handled by wrapper propagation. Values
	// of function type are opaque: their target can change at runtime.
	indirect := false
	if _, ok := p.underlyingType(p.expressionType(call.Fun, map[ast.Node]bool{}), map[string]bool{}).(*ast.FuncType); ok {
		indirect = true
		if id, ok := call.Fun.(*ast.Ident); ok {
			if id.Obj != nil && id.Obj.Kind == ast.Fun {
				indirect = false
			}
			if id.Obj == nil {
				for _, f := range p.files {
					for _, decl := range f.Decls {
						if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == id.Name {
							indirect = false
						}
					}
				}
			}
		}
	}
	if selector, ok := call.Fun.(*ast.SelectorExpr); ok && p.methodDeclaration(selector) != nil {
		indirect = false
	}
	// Passing an SQL method as a value is itself a boundary crossing, even
	// when the receiving helper is declared in another source file.
	for _, arg := range call.Args {
		if _, ok := arg.(*ast.SelectorExpr); ok {
			if _, sql := linkSQLMethod(linkCallName(arg)); sql {
				indirect = true
			}
		}
	}
	indexes := map[int]bool{}
	if indirect {
		for i, arg := range call.Args {
			_, constant := p.constant(file, arg, map[string]bool{})
			typ := p.underlyingType(p.expressionType(arg, map[ast.Node]bool{}), map[string]bool{})
			id, _ := typ.(*ast.Ident)
			if constant || (id != nil && id.Name == "string") {
				indexes[i] = true
			}
		}
	}
	return indexes
}

// Resolve declared methods only to distinguish them from function fields and
// to follow methods whose result is a callable value.
func (p *linkSQLPackage) methodDeclaration(selector *ast.SelectorExpr) *ast.FuncDecl {
	typ := p.expressionType(selector.X, map[ast.Node]bool{})
	if pointer, ok := typ.(*ast.StarExpr); ok {
		typ = pointer.X
	}
	id, ok := typ.(*ast.Ident)
	if !ok {
		return nil
	}
	for _, file := range p.files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Name.Name != selector.Sel.Name {
				continue
			}
			receiver := fn.Recv.List[0].Type
			if pointer, ok := receiver.(*ast.StarExpr); ok {
				receiver = pointer.X
			}
			if r, ok := receiver.(*ast.Ident); ok && r.Name == id.Name {
				return fn
			}
		}
	}
	return nil
}
