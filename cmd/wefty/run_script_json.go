package main

import "github.com/Derek-X-Wang/wefty/contract"

// These views change only CLI JSON presentation. Embedded records preserve
// all other fields, and the stored/API snapshot remains complete.
type runRecordJSON struct {
	contract.RunRecord
	Workflow workflowSourceJSON `json:"workflow"`
}

type workflowSourceJSON struct {
	contract.WorkflowSource
	InlineScript *inlineScriptJSON `json:"inline_script,omitempty"`
}

type inlineScriptJSON struct {
	Content        *string `json:"content,omitempty"`
	SHA256         string  `json:"sha256"`
	SizeBytes      *int    `json:"size_bytes,omitempty"`
	ContentOmitted bool    `json:"content_omitted,omitempty"`
}

func newRunRecordJSON(record contract.RunRecord, includeScript bool) runRecordJSON {
	view := runRecordJSON{
		RunRecord: record,
		Workflow:  workflowSourceJSON{WorkflowSource: record.Workflow},
	}
	if script := record.Workflow.InlineScript; script != nil {
		inline := &inlineScriptJSON{SHA256: script.SHA256}
		if includeScript {
			inline.Content = &script.Content
		} else {
			size := len(script.Content)
			inline.SizeBytes = &size
			inline.ContentOmitted = true
		}
		view.Workflow.InlineScript = inline
	}
	return view
}

func newRunRecordsJSON(records []contract.RunRecord, includeScript bool) []runRecordJSON {
	if records == nil {
		return nil
	}
	views := make([]runRecordJSON, len(records))
	for i, record := range records {
		views[i] = newRunRecordJSON(record, includeScript)
	}
	return views
}
