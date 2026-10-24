package scripts

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// Mutation waits share one failure decision. Run wait and the three log
// followers retain their separate policies; one-shot GET retries stay local.
func TestCLIWaitDecisionCallSites(t *testing.T) {
	allowed := map[string]bool{
		"seat_logs_command.go:executeSeatLogs:isRetryableObservation":           true,
		"seat_logs_command.go:executeSeatLogs:isRetryableL1Answer":              true,
		"exec_command.go:submitExecRun:isRetryableL1Answer":                     true,
		"wait_decision.go:decideWaitFailure:isRetryableL1Answer":                true,
		"client.go:do:isRetryableL1Answer":                                      true,
		"wait_decision.go:isRetryableObservation:isRetryableL1Answer":           true,
		"command.go:executeLogs:isRetryableObservation":                         true,
		"command.go:executeLogs:isRetryableL1Answer":                            true,
		"service_command.go:executeServiceLogs:isRetryableL1Answer":             true,
		"computer_storage_command.go:pollStorageObservation:decideWaitFailure":  true,
		"computer_storage_command.go:attachStorageProvenance:decideWaitFailure": true,
		"service_command.go:waitForService:decideWaitFailure":                   true,
		"access_command.go:executeComputerGrant:decideWaitFailure":              true,
		"computer_storage_command.go:pollStorageObservation:openWaitWindow":     true,
		"service_command.go:waitForService:openWaitWindow":                      true,
		"access_command.go:executeComputerGrant:openWaitWindow":                 true,
	}
	files, err := filepath.Glob("../cmd/wefty/*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			site := filepath.Base(path) + ":" + fn.Name.Name
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				name, ok := call.Fun.(*ast.Ident)
				if !ok {
					return true
				}
				switch name.Name {
				case "isRetryableL1Answer", "isRetryableObservation", "decideWaitFailure", "openWaitWindow":
					key := site + ":" + name.Name
					if _, ok := allowed[key]; !ok {
						t.Errorf("unapproved wait helper call: %s", key)
					} else {
						allowed[key] = false
					}
				}
				return true
			})
		}
	}
	for site, missing := range allowed {
		if missing {
			t.Errorf("missing wait helper call: %s", site)
		}
	}
}
