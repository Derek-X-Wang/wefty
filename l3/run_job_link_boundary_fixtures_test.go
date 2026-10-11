package l3

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type linkBypassFixture struct {
	name, source string
	want         int
}

func runJobLinkBypassFixtures() []linkBypassFixture {
	return []linkBypassFixture{
		{"function-parameter", `func read(q func(string) (*sql.Rows,error)){q("SELECT l1_job_id FROM runs")}`, 1},
		{"sql-handle-call-argument", `func read(q func(context.Context,string,...any)*sql.Row,ctx context.Context,id string){q(ctx,"SELECT l1_job_id FROM runs WHERE run_id=?",id)};func f(s *Store,ctx context.Context){read(s.db.QueryRowContext,ctx,"run")}`, 2},
		{"function-field-literal", `type h struct {q func(string,...any)(*sql.Rows,error)};func f(db *sql.DB){x:=h{q:db.Query};x.q("SELECT l1_job_id FROM runs")}`, 1},
		{"function-field-assignment", `type h struct {q func(string,...any)(*sql.Rows,error)};func f(db *sql.DB){var x h;x.q=db.Query;x.q("SELECT l1_job_id FROM runs")}`, 1},
		{"function-method-result", `type h struct{};func(x h)get(db *sql.DB)func(string,...any)(*sql.Rows,error){return db.Query};func f(x h,db *sql.DB){x.get(db)("SELECT l1_job_id FROM runs")}`, 1},
		{"named-function-parameter", `type query func(string);func f(q query){q("SELECT l1_job_id FROM runs")}`, 1},
		{"replace-without-into", `func f(db *sql.DB){db.Exec("REPLACE runs(run_id,status) VALUES(?,?)")}`, 1},
		{"function-result", `func get(db *sql.DB)func(string,...any)(*sql.Rows,error){return db.Query};func f(db *sql.DB){get(db)("SELECT l1_job_id FROM runs")}`, 1},
		{"function-local", `func f(db *sql.DB){q:=func(s string){db.Query(s)};q("SELECT l1_job_id FROM runs")}`, 2},
		{"opaque-function-safe-string", `func f(q func(string)){q("SELECT status FROM runs")}`, 1},
		{"function-no-string-positive", `func f(q func(int)){q(2)}`, 0},
		{"ordinary-function-positive", `func read(s string){};func f(){read("ordinary string")}`, 0},
		{"ordinary-method-positive", `type h struct{};func(x h)q(s string){};func f(x h){x.q("ordinary string")}`, 0},
		{"replace-runs", `func f(db *sql.DB){db.Exec("REPLACE INTO runs(run_id,status) VALUES(?,?)")}`, 1},
		{"insert-or-replace-outbox", `func f(db *sql.DB){db.Exec("INSERT OR REPLACE INTO dispatch_outbox(run_id,dispatch_key) VALUES(?,?)")}`, 1},
		{"positional-insert-outbox", `func f(db *sql.DB){db.Exec("INSERT INTO dispatch_outbox VALUES(?,?,?,?,?)")}`, 1},
		{"positional-insert-runs", `func f(db *sql.DB){db.Exec("INSERT INTO main.runs VALUES(?,?,?,?,?)")}`, 1},
		{"unprotected-replace-positive", `func f(db *sql.DB){db.Exec("INSERT OR REPLACE INTO other(run_id) VALUES(?)")}`, 0},
		{"explicit-unprotected-insert-positive", `func f(db *sql.DB){db.Exec("INSERT INTO runs(run_id,status) VALUES(?,?)")}`, 0},
		{"ownership-count-one", `func f(db *sql.DB){db.Query("SELECT COUNT(1) FROM run_cancellations WHERE run_id=?")}`, 1},
		{"ownership-select-two", `func f(db *sql.DB){db.Query("SELECT 2 FROM run_cancellations WHERE run_id=?")}`, 1},
		{"ownership-left-join", `func f(db *sql.DB){db.Query("SELECT r.status FROM runs r LEFT JOIN run_cancellations c ON c.run_id=r.run_id WHERE c.requested_ns IS NULL")}`, 1},
		{"ownership-scalar-subquery", `func f(db *sql.DB){db.Query("SELECT (SELECT requested_ns FROM run_cancellations WHERE run_id=r.run_id) FROM runs r")}`, 1},
		{"ownership-delivery-column", `func f(db *sql.DB){db.Query("SELECT failures FROM run_cancellations WHERE run_id=?")}`, 1},
		{"delivery-unfiltered-positive", `func f(db *sql.DB){db.Query("SELECT failures FROM run_cancellations")}`, 0},
		{"constant-concatenation", `func f(db *sql.DB){db.Query("SELECT "+"l1_job_id FROM runs")}`, 1},
		{"cross-file-constant", `func f(db *sql.DB){db.Query(linkRead)}`, 1},
		{"local-constant", `func f(db *sql.DB){const q="SELECT l1_job_id FROM runs";db.Query(q)}`, 1},
		{"imported-constant", `func f(db *sql.DB){db.Query(foreign.LinkRead)}`, 1},
		{"mutable-assembly", `func f(db *sql.DB){q:="SELECT ";q+="l1_job_id FROM runs";db.Query(q)}`, 1},
		{"aliased-handle", `func f(db *sql.DB){other:=db;other.Query("SELECT r.l1_job_id FROM runs AS r")}`, 1},
		{"helper-returned-sql", `func query()string{return "SELECT l1_job_id FROM runs"};func f(db *sql.DB){db.Query(query())}`, 1},
		{"quoted-identifiers", "func f(db *sql.DB){db.Query(\"SELECT \\\"r\\\".\\\"l1_job_id\\\" FROM \\\"runs\\\" AS \\\"r\\\"\")}", 1},
		{"sqlite-single-quoted-table", `func f(db *sql.DB){db.Query("SELECT * FROM 'runs'")}`, 1},
		{"sqlite-single-quoted-wildcard", `func f(db *sql.DB){db.Query("SELECT 'r'.* FROM 'runs' AS 'r'")}`, 1},
		{"sqlite-single-quoted-column", `func f(db *sql.DB){db.Query("SELECT 'r'.'l1_job_id' FROM 'runs' AS 'r'")}`, 1},
		{"scalar-string-positive", `func f(db *sql.DB){db.Query("SELECT 'l1_job_id' FROM runs")}`, 0},
		{"bracket-identifiers", `func f(db *sql.DB){db.Query("SELECT [r].[job_link_settled] FROM [runs] [r]")}`, 1},
		{"wildcard", `func f(db *sql.DB){db.Query("SELECT * FROM runs")}`, 1},
		{"aliased-wildcard", `func f(db *sql.DB){db.Query("SELECT r.* FROM runs r")}`, 1},
		{"subquery", `func f(db *sql.DB){db.Query("SELECT value FROM other WHERE EXISTS (SELECT r.l1_job_id FROM runs r)")}`, 1},
		{"comma-join", `func f(db *sql.DB){db.Query("SELECT r.l1_job_id FROM other x, runs r")}`, 1},
		{"ownership-exists", `func f(db *sql.DB){db.Query("SELECT EXISTS(SELECT 1 FROM run_cancellations WHERE run_id=?)")}`, 1},
		{"ownership-insert", `func f(db *sql.DB){db.Exec("INSERT INTO run_cancellations(run_id) VALUES(?)")}`, 1},
		{"ownership-key-update", `func f(db *sql.DB){db.Exec("UPDATE run_cancellations SET run_id=? WHERE run_id=?")}`, 1},
		{"ownership-replace", `func f(db *sql.DB){db.Exec("REPLACE INTO run_cancellations(run_id) VALUES(?)")}`, 1},
		{"ownership-delete", `func f(db *sql.DB){db.Exec("DELETE FROM run_cancellations WHERE run_id=?")}`, 1},
		{"method-value", `func f(db *sql.DB){query:=db.Query;query("SELECT * FROM dispatch_outbox")}`, 1},
		{"prepared", `func f(db *sql.DB){statement,_:=db.Prepare("SELECT l1_job_id FROM runs");statement.Query()}`, 2},
		{"builder-exec", `func f(db *sql.DB){builder.Select("l1_job_id").From("runs").RunWith(db).Query()}`, 1},
		{"builder", `func f(db *sql.DB){db.Query(builder.Select("l1_job_id").From("runs").SQL())}`, 1},
		{"opaque-wrapper", `func execute(db *sql.DB,q string){db.Exec(q)};func f(db *sql.DB){execute(db,"SELECT l1_job_id FROM runs")}`, 2},
		{"wrapper-alias", `func execute(db *sql.DB,q string){sql:=q;db.Exec(sql)};func f(db *sql.DB){execute(db,"SELECT l1_job_id FROM runs")}`, 2},
		{"wrapper-method-value", `func execute(db *sql.DB,q string){query:=db.Query;query(q)};func f(db *sql.DB){execute(db,"SELECT l1_job_id FROM runs")}`, 2},
		{"wrapper-constructor-method-value", `func execute(db *sql.DB,table,column string){db.Query("SELECT "+column+" FROM "+table)};func f(db *sql.DB){exec:=execute;exec(db,"runs","l1_job_id")}`, 2},
		{"global-method-value", `var query=(*sql.DB).Query;func f(db *sql.DB){query(db,"SELECT l1_job_id FROM runs")}`, 1},
		{"var-method-value", `func f(db *sql.DB){var query=db.Query;query("SELECT l1_job_id FROM runs")}`, 1},
		{"wrapper-identifier-construction", `func execute(db *sql.DB,table,column string){db.Query("SELECT "+column+" FROM "+table)};func f(db *sql.DB){execute(db,"runs","l1_job_id")}`, 2},
		{"wrapper-unrelated-positive", `func execute(db *sql.DB,q string){db.Query(q)};func f(db *sql.DB){execute(db,"SELECT job_id FROM other")}`, 1},
		{"wrapper-chain", `func execute(db *sql.DB,q string){db.Exec(q)};func forward(db *sql.DB,q string){execute(db,q)};func f(db *sql.DB){forward(db,"SELECT l1_job_id FROM runs")}`, 3},
		{"shadowed-constant", `const q="SELECT value FROM other";func f(db *sql.DB){q:="SELECT l1_job_id FROM runs";db.Query(q)}`, 1},
		{"url-query-positive", `func f(req *http.Request){_=req.URL.Query()}`, 0},
		{"url-type-query-positive", `func f(u *url.URL){_=u.Query()}`, 0},
		{"unrelated-job-id", `func f(db *sql.DB){db.Query("SELECT job_id FROM other")}`, 0},
		{"unrelated-qualified-job-id", `func f(db *sql.DB){db.Query("SELECT x.job_id FROM other x JOIN dispatch_outbox o ON o.run_id=x.run_id")}`, 0},
		{"unrelated-run-outcome", `func f(db *sql.DB){db.Query("SELECT status FROM runs")}`, 0},
		{"unrelated-row-count", `func f(db *sql.DB){db.Query("SELECT COUNT(*) FROM runs")}`, 0},
		{"delivery-read", `func f(db *sql.DB){db.Query("SELECT failures FROM run_cancellations WHERE run_id=?")}`, 1},
		{"delivery-write", `func f(db *sql.DB){db.Exec("UPDATE run_cancellations SET completed_ns=? WHERE run_id=?")}`, 0},
		{"imported-unrelated", `func f(db *sql.DB){db.Query(foreign.OtherRead)}`, 0},
	}
}

func TestRunJobLinkBoundaryBypassFixtures(t *testing.T) {
	for _, test := range runJobLinkBypassFixtures() {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Mkdir(filepath.Join(root, "foreign"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "foreign", "sql.go"), []byte(`package foreign;const LinkRead="SELECT l1_job_id FROM runs";const OtherRead="SELECT job_id FROM other"`), 0600); err != nil {
				t.Fatal(err)
			}
			sources := map[string]string{"fixture.go": `package fixture; import "database/sql";import "net/http";import "net/url";import "github.com/Derek-X-Wang/wefty/foreign";` + test.source, "constant.go": `package fixture;const linkRead="SELECT l1_job_id FROM runs"`}
			p, err := parseLinkSQLPackage(root, sources)
			if err != nil {
				t.Fatal(err)
			}
			if got := p.sites(); len(got) != test.want {
				t.Fatalf("sites=%+v want %d", got, test.want)
			}
			// Positive module control: the same SQL is allowed only inside the module.
			delete(sources, "fixture.go")
			sources["run_job_link.go"] = `package fixture;import "database/sql";` + test.source
			p, err = parseLinkSQLPackage(root, sources)
			if err != nil {
				t.Fatal(err)
			}
			if got := p.sites(); len(got) != 0 {
				t.Fatalf("module SQL was refused: %+v", got)
			}
		})
	}
}

func TestRunJobLinkBoundaryExactRatchet(t *testing.T) {
	site := linkBoundaryEntry{File: "fixture.go", Function: "f", Fingerprint: "abc", Count: 1}
	allowed := site
	allowed.Reason = "reader adoption"
	allowed.Retirement = "#783"
	if err := checkLinkBoundaryRatchet([]linkBoundaryEntry{site}, []linkBoundaryEntry{allowed}); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name            string
		actual, allowed []linkBoundaryEntry
	}{
		{"unexpected", []linkBoundaryEntry{site}, nil},
		{"obsolete", nil, []linkBoundaryEntry{allowed}},
		{"duplicate-entry", []linkBoundaryEntry{site}, []linkBoundaryEntry{allowed, allowed}},
		{"missing-reason", []linkBoundaryEntry{site}, []linkBoundaryEntry{site}},
	}
	changed := site
	changed.Fingerprint = "different"
	cases = append(cases, struct {
		name            string
		actual, allowed []linkBoundaryEntry
	}{"changed", []linkBoundaryEntry{changed}, []linkBoundaryEntry{allowed}})
	duplicated := site
	duplicated.Count = 2
	cases = append(cases, struct {
		name            string
		actual, allowed []linkBoundaryEntry
	}{"duplicate-occurrence", []linkBoundaryEntry{duplicated}, []linkBoundaryEntry{allowed}})
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if err := checkLinkBoundaryRatchet(test.actual, test.allowed); err == nil {
				t.Fatal("ratchet accepted mismatch")
			}
		})
	}
}

// Run the same scanner and exact ratchet as the real repository gate, adding
// each review bypass as a new production source. Positive controls must leave
// the ratchet unchanged. Parser fixtures may deliberately use unresolved types.
func TestRunJobLinkBoundaryProductionBypasses(t *testing.T) {
	sources := map[string]string{}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".go") {
			body, err := os.ReadFile(entry.Name())
			if err != nil {
				t.Fatal(err)
			}
			sources[entry.Name()] = string(body)
		}
	}
	body, err := os.ReadFile("testdata/run_job_link_boundary.json")
	if err != nil {
		t.Fatal(err)
	}
	var allowed []linkBoundaryEntry
	if err := json.Unmarshal(body, &allowed); err != nil {
		t.Fatal(err)
	}
	p, err := parseLinkSQLPackage("..", sources)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkLinkBoundaryRatchet(p.sites(), allowed); err != nil {
		t.Fatal(err)
	}
	for _, test := range runJobLinkBypassFixtures() {
		if test.name == "constant-concatenation" {
			break
		} // Review's new bypass family.
		t.Run(test.name, func(t *testing.T) {
			sources["zz_review_bypass.go"] = "package l3;" + test.source
			defer delete(sources, "zz_review_bypass.go")
			p, err := parseLinkSQLPackage("..", sources)
			if err != nil {
				t.Fatal(err)
			}
			err = checkLinkBoundaryRatchet(p.sites(), allowed)
			if test.want > 0 && err == nil {
				t.Fatal("production bypass passed the real storage ratchet")
			}
			if test.want == 0 && err != nil {
				t.Fatalf("positive control changed storage ratchet: %v", err)
			}
		})
	}
}
