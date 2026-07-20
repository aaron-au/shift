package store_test

import (
	"testing"

	"github.com/aaron-au/shift/hub/internal/store"
)

func TestDirectExecutions(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	if _, err := s.RecordDirectExecution(ctx, "", store.DirectExecution{
		FlowName: "f1", Trigger: "webhook", State: "completed", RecordsIn: 10, RecordsOut: 8,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordDirectExecution(ctx, "", store.DirectExecution{
		FlowName: "f2", Trigger: "api", State: "failed", Error: "boom",
	}); err != nil {
		t.Fatal(err)
	}

	list, err := s.DirectExecutions(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 {
		t.Fatalf("list = %d, want 2", len(list))
	}
	// Newest first.
	if list[0].FlowName != "f2" || list[0].State != "failed" || list[0].Error != "boom" {
		t.Fatalf("list[0] = %+v", list[0])
	}
	if list[1].FlowName != "f1" || list[1].RecordsIn != 10 || list[1].RecordsOut != 8 {
		t.Fatalf("list[1] = %+v", list[1])
	}
}
