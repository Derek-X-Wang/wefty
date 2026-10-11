package l3

import (
	"testing"
)

type linkReviewProbe struct {
	name  string
	files map[string]string
}

const linkReviewHeader = `package fixture; import "database/sql";import "fmt";import "context";`

func linkReviewProbes() []linkReviewProbe {
	return []linkReviewProbe{
		{"A-local-var", map[string]string{"fixture.go": linkReviewHeader + `func f(db *sql.DB){q:="SELECT l1_job_id FROM runs";db.Query(q)}`}},
		{"B-named-result-bare-return", map[string]string{"fixture.go": linkReviewHeader + `func q(b bool)(s string){if b{s="SELECT l1_job_id FROM runs";return};return "SELECT status FROM runs"};func f(db *sql.DB,b bool){db.Query(q(b))}`}},
		{"B2-named-result-only-bare", map[string]string{"fixture.go": linkReviewHeader + `func q()(s string){s="SELECT l1_job_id FROM runs";return};func f(db *sql.DB){db.Query(q())}`}},
		{"B3-helper-defer-mutates-named", map[string]string{"fixture.go": linkReviewHeader + `func q()(s string){defer func(){s="SELECT l1_job_id FROM runs"}();return "SELECT status FROM runs"};func f(db *sql.DB){db.Query(q())}`}},
		{"C-helper-sprintf", map[string]string{"fixture.go": linkReviewHeader + `func q()string{return fmt.Sprintf("SELECT %s FROM runs","l1_job_id")};func f(db *sql.DB){db.Query(q())}`}},
		{"D-loop-order-split-token", map[string]string{"fixture.go": linkReviewHeader + `func f(db *sql.DB){q:="SELECT status FROM runs WHERE ";for i:=0;i<2;i++{if i==1{q+="job_id IS NULL"}else{q+="l1_"}};db.Query(q)}`}},
		{"D2-goto-order", map[string]string{"fixture.go": linkReviewHeader + `func f(db *sql.DB){q:="SELECT ";goto a;b: q+="job_id FROM runs";db.Query(q);return;a: q+="l1_";goto b}`}},
		{"E-wrapper-fnparam-outside", map[string]string{"fixture.go": linkReviewHeader + `func execute(db *sql.DB,q string){db.Query(q)};func apply(fn func(*sql.DB,string),db *sql.DB){fn(db,"SELECT l1_job_id FROM runs")};func f(db *sql.DB){apply(execute,db)}`}},
		{"E0-wrapper-only", map[string]string{"fixture.go": linkReviewHeader + `func execute(db *sql.DB,q string){db.Query(q)};func f(db *sql.DB){execute(db,"SELECT l1_job_id FROM runs")}`}},
		{"E1-module-wrapper-direct", map[string]string{"run_job_link.go": linkReviewHeader + `func execute(db *sql.DB,q string){db.Query(q)}`, "fixture.go": linkReviewHeader + `func f(db *sql.DB){execute(db,"SELECT l1_job_id FROM runs")}`}},
		{"E2-module-wrapper-map", map[string]string{"run_job_link.go": linkReviewHeader + `func execute(db *sql.DB,q string){db.Query(q)}`, "fixture.go": linkReviewHeader + `func f(db *sql.DB){m:=map[string]func(*sql.DB,string){"x":execute};m["x"](db,"SELECT l1_job_id FROM runs")}`}},
		{"E3-module-wrapper-callback", map[string]string{"run_job_link.go": linkReviewHeader + `func execute(db *sql.DB,q string){db.Query(q)}`, "fixture.go": linkReviewHeader + `func apply(fn func(*sql.DB,string),db *sql.DB){fn(db,"SELECT l1_job_id FROM runs")};func f(db *sql.DB){apply(execute,db)}`}},
		{"F-delete-runs", map[string]string{"fixture.go": linkReviewHeader + `func f(db *sql.DB){db.Exec("DELETE FROM runs WHERE run_id=?")}`}},
		{"F2-delete-outbox", map[string]string{"fixture.go": linkReviewHeader + `func f(db *sql.DB){db.Exec("DELETE FROM dispatch_outbox WHERE run_id=?")}`}},
		{"F3-insert-or-replace", map[string]string{"fixture.go": linkReviewHeader + `func f(db *sql.DB){db.Exec("INSERT OR REPLACE INTO dispatch_outbox(run_id,dispatch_key) VALUES(?,?)")}`}},
		{"F4-replace-schema", map[string]string{"fixture.go": linkReviewHeader + `func f(db *sql.DB){db.Exec("REPLACE INTO main.runs(run_id,status) VALUES(?,?)")}`}},
		{"F5-update-run-key", map[string]string{"fixture.go": linkReviewHeader + `func f(db *sql.DB){db.Exec("UPDATE dispatch_outbox SET run_id=? WHERE run_id=?")}`}},
		{"G-natural-join-outbox", map[string]string{"fixture.go": linkReviewHeader + `func f(db *sql.DB){db.Query("SELECT o.dispatch_key FROM dispatch_outbox o NATURAL JOIN run_cancellations c")}`}},
		{"G2-natural-join-triggers", map[string]string{"fixture.go": linkReviewHeader + `func f(db *sql.DB){db.Query("SELECT t.actor FROM run_triggers t NATURAL JOIN run_cancellations")}`}},
		{"G3-update-returning", map[string]string{"fixture.go": linkReviewHeader + `func f(db *sql.DB){db.QueryRow("UPDATE run_cancellations SET failures=failures WHERE run_id=? RETURNING 1")}`}},
		{"G4-unscoped-select", map[string]string{"fixture.go": linkReviewHeader + `func f(db *sql.DB){db.Query("SELECT requested_ns FROM run_cancellations")}`}},
		{"G5-delivery-update-rows", map[string]string{"fixture.go": linkReviewHeader + `func f(db *sql.DB){r,_:=db.Exec("UPDATE run_cancellations SET retry_ns=retry_ns WHERE run_id=?");r.RowsAffected()}`}},
		{"H-recovery-file", map[string]string{"run_job_link_recovery.go": linkReviewHeader + `func f(db *sql.DB){db.Query("SELECT l1_job_id FROM runs")}`}},
		{"H2-schedule-file", map[string]string{"recovery_schedule.go": linkReviewHeader + `func f(db *sql.DB){db.Query("SELECT l1_job_id FROM runs")}`}},
		{"I-struct-method-value", map[string]string{"fixture.go": linkReviewHeader + `type h struct{q func(string,...any)(*sql.Rows,error)};func f(db *sql.DB){x:=h{q:db.Query};x.q("SELECT l1_job_id FROM runs")}`}},
		{"J-fragment-forward", map[string]string{"fixture.go": linkReviewHeader + `func ensure(db *sql.DB,table,column string){db.Exec("ALTER TABLE "+table+" ADD COLUMN "+column)};func add(db *sql.DB,table,column string){ensure(db,table,column)};func f(db *sql.DB){add(db,"other","x")}`}},
		{"J2-fragment-forward-protected", map[string]string{"fixture.go": linkReviewHeader + `func ensure(db *sql.DB,table,column string){db.Exec("ALTER TABLE "+table+" ADD COLUMN "+column)};func add(db *sql.DB,table,column string){ensure(db,table,column)};func f(db *sql.DB){add(db,"other","x")};func g(db *sql.DB){add(db,"runs","l1_job_id")}`}},
		{"J3-module-opaque-forward", map[string]string{"run_job_link.go": linkReviewHeader + `func ensure(db *sql.DB,table,column string){db.Exec("ALTER TABLE "+table+" ADD COLUMN "+column)};func add(db *sql.DB,table,column string){ensure(db,table,column)}`, "fixture.go": linkReviewHeader + `func f(db *sql.DB){add(db,"other","x")};func g(db *sql.DB){add(db,"runs","l1_job_id")}`}},
		{"K-method-wrapper-callback", map[string]string{"fixture.go": linkReviewHeader + `type S struct{db *sql.DB};func (s *S) q(query string){s.db.Query(query)};func apply(fn func(string)){fn("SELECT l1_job_id FROM runs")};func f(s *S){apply(s.q)}`}},
		{"L-closure-var-reassign", map[string]string{"fixture.go": linkReviewHeader + `func f(db *sql.DB){q:="SELECT status FROM runs";set:=func(v string){q=v};set("SELECT l1_job_id FROM runs");db.Query(q)}`}},
		{"M-ctx-wrapper-pkgvar-func", map[string]string{"fixture.go": linkReviewHeader + `var exec=func(db *sql.DB,q string){db.Query(q)};func f(db *sql.DB){exec(db,"SELECT l1_job_id FROM runs")}`}},
		{"N-multi-assign-swap", map[string]string{"fixture.go": linkReviewHeader + `func f(db *sql.DB){a,b:="SELECT status FROM runs","SELECT l1_job_id FROM runs";a,b=b,a;db.Query(a)}`}},
		{"O-param-closure-callers", map[string]string{"fixture.go": linkReviewHeader + `func run(db *sql.DB,q string){db.Query(q)};func f(db *sql.DB){run(db,"SELECT status FROM runs");defer run(db,"SELECT l1_job_id FROM runs")}`}},
		{"P-go-stmt-call", map[string]string{"fixture.go": linkReviewHeader + `func run(db *sql.DB,q string){db.Query(q)};func f(db *sql.DB){run(db,"SELECT status FROM runs");go run(db,"SELECT l1_job_id FROM runs")}`}},
		{"Q-variadic-spread", map[string]string{"fixture.go": linkReviewHeader + `func run(db *sql.DB,q string, a ...any){db.Query(q, a...)};func f(db *sql.DB){args:=[]any{1};run(db,"SELECT status FROM runs",args...)}`}},
		{"R-method-recv-private-helper", map[string]string{"fixture.go": linkReviewHeader + `type S struct{db *sql.DB};func (s *S) q(query string){s.db.Query(query)};type T struct{};func (T) q(query string){};func f(s *S,t T){s.q("SELECT status FROM runs");t.q("SELECT l1_job_id FROM runs")}`}},
		{"S-iface-call-private-method", map[string]string{"fixture.go": linkReviewHeader + `type qer interface{q(string)};type S struct{db *sql.DB};func (s *S) q(query string){s.db.Query(query)};func f(s *S){var x qer=s;s.q("SELECT status FROM runs");x.q("SELECT l1_job_id FROM runs")}`}},
		{"T-method-expression-private", map[string]string{"fixture.go": linkReviewHeader + `type S struct{db *sql.DB};func (s *S) q(query string){s.db.Query(query)};func f(s *S){s.q("SELECT status FROM runs");(*S).q(s,"SELECT l1_job_id FROM runs")}`}},
	}
}

func TestRunJobLinkReviewProbes(t *testing.T) {
	want := map[string]int{
		"A-local-var":                   1,
		"B-named-result-bare-return":    1,
		"B2-named-result-only-bare":     1,
		"B3-helper-defer-mutates-named": 1,
		"C-helper-sprintf":              1,
		"D-loop-order-split-token":      1,
		"D2-goto-order":                 1,
		"E-wrapper-fnparam-outside":     3,
		"E0-wrapper-only":               2,
		"E1-module-wrapper-direct":      1,
		"E2-module-wrapper-map":         1,
		"E3-module-wrapper-callback":    2,
		"F-delete-runs":                 1,
		"F2-delete-outbox":              1,
		"F3-insert-or-replace":          1,
		"F4-replace-schema":             1,
		"F5-update-run-key":             0,
		"G-natural-join-outbox":         1,
		"G2-natural-join-triggers":      1,
		"G3-update-returning":           1,
		"G4-unscoped-select":            0,
		"G5-delivery-update-rows":       0,
		"H-recovery-file":               1,
		"H2-schedule-file":              1,
		"I-struct-method-value":         1,
		"J-fragment-forward":            0,
		"J2-fragment-forward-protected": 4,
		"J3-module-opaque-forward":      2,
		"K-method-wrapper-callback":     3,
		"L-closure-var-reassign":        1,
		"M-ctx-wrapper-pkgvar-func":     1,
		"N-multi-assign-swap":           1,
		"O-param-closure-callers":       2,
		"P-go-stmt-call":                2,
		"Q-variadic-spread":             0,
		"R-method-recv-private-helper":  2,
		"S-iface-call-private-method":   2,
		"T-method-expression-private":   2,
	}
	for _, c := range linkReviewProbes() {
		t.Run(c.name, func(t *testing.T) {
			p, err := parseLinkSQLPackage(t.TempDir(), c.files)
			if err != nil {
				t.Fatal(err)
			}
			expected, ok := want[c.name]
			if !ok {
				t.Fatal("probe is missing an explicit site expectation")
			}
			if sites := p.sites(); len(sites) != expected {
				t.Fatalf("sites=%+v want %d", sites, expected)
			}
		})
	}
}
