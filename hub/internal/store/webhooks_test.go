package store_test

import (
	"errors"
	"testing"

	"github.com/aaron-au/shift/hub/internal/store"
)

func TestWebhooks(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	deployPublished(t, s, "orders")

	// Bind a hook to the published flow.
	if err := s.UpsertWebhook(ctx, store.Webhook{
		Name: "hook1", FlowName: "orders", TokenHash: "abc123", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	// Unknown flow is rejected.
	if err := s.UpsertWebhook(ctx, store.Webhook{Name: "h", FlowName: "ghost", Enabled: true}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown flow: %v", err)
	}

	list, err := s.Webhooks(ctx)
	if err != nil || len(list) != 1 || list[0].Name != "hook1" || !list[0].Enabled {
		t.Fatalf("list = %+v, %v", list, err)
	}

	// Sync configs include the published document + token hash.
	cfgs, err := s.EnabledWebhookConfigs(ctx)
	if err != nil || len(cfgs) != 1 {
		t.Fatalf("configs = %+v, %v", cfgs, err)
	}
	if cfgs[0].Name != "hook1" || cfgs[0].TokenHash != "abc123" || len(cfgs[0].Document) == 0 {
		t.Fatalf("config = %+v", cfgs[0])
	}

	// Disabled hooks drop out of sync.
	if err := s.UpsertWebhook(ctx, store.Webhook{Name: "hook1", FlowName: "orders", Enabled: false}); err != nil {
		t.Fatal(err)
	}
	if cfgs, _ := s.EnabledWebhookConfigs(ctx); len(cfgs) != 0 {
		t.Fatalf("disabled hook still synced: %+v", cfgs)
	}

	// Delete is idempotent-checked.
	if err := s.DeleteWebhook(ctx, "hook1"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteWebhook(ctx, "hook1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("double delete: %v", err)
	}
}
