package l3

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRunJobLinkBoundaryBypassFixtures(t *testing.T) {
	tests := []struct {
		name, source string
		want         int
	}{
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
		{"delivery-read", `func f(db *sql.DB){db.Query("SELECT failures FROM run_cancellations WHERE run_id=?")}`, 0},
		{"delivery-write", `func f(db *sql.DB){db.Exec("UPDATE run_cancellations SET completed_ns=? WHERE run_id=?")}`, 0},
		{"imported-unrelated", `func f(db *sql.DB){db.Query(foreign.OtherRead)}`, 0},
	}
	for _, test := range tests {
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
