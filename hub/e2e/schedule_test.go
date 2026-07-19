package e2e

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/api"
	"github.com/aaron-au/shift/hub/internal/pgtest"
	"github.com/aaron-au/shift/hub/internal/scheduler"
	"github.com/aaron-au/shift/hub/internal/store"
)

const fastFlow = `{"name":"tick",
  "source":{"connector":"gen","action":"gen","config":{"records":10}},
  "sink":{"connector":"gen","action":"discard"}}`

// TestScheduleFiresExactlyOnce runs TWO complete hub replicas (store +
// API + scheduler loop) against one Postgres and proves the M4b exit
// criterion: each schedule tick becomes exactly one task, no matter how
// many replicas race, including across a mid-pass replica shutdown.
func TestScheduleFiresExactlyOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	dsn := pgtest.DSN(t)

	type replica struct {
		st    *store.Store
		hub   *httptest.Server
		sched *scheduler.Scheduler
		stop  func()
	}
	newReplica := func(migrate bool) replica {
		st, err := store.Open(t.Context(), dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(st.Close)
		if migrate {
			if err := st.Migrate(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
		h, err := api.Handler(st, api.Options{AdminToken: adminToken})
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(h)
		t.Cleanup(srv.Close)

		sched := scheduler.New(st, scheduler.Options{Interval: 100 * time.Millisecond})
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() { defer close(done); sched.Run(ctx) }()
		var once sync.Once
		stop := func() { once.Do(func() { cancel(); <-done }) }
		t.Cleanup(stop)
		return replica{st: st, hub: srv, sched: sched, stop: stop}
	}

	r1 := newReplica(true)
	r2 := newReplica(false)

	// Deploy + publish + schedule every minute via replica 1's API.
	doJSON(t, r1.hub.URL, "PUT", "/api/v1/flows/tick", fastFlow, nil)
	doJSON(t, r1.hub.URL, "POST", "/api/v1/flows/tick/versions/1/publish", "", nil)
	doJSON(t, r1.hub.URL, "PUT", "/api/v1/flows/tick/schedule", `{"cron":"* * * * *"}`, nil)

	// Backdate the schedule so the next passes (on both replicas) see it
	// due immediately — no minute-long test.
	backdate := func() {
		if _, err := r1.st.UpsertSchedule(t.Context(), "tick", "* * * * *", true, 3,
			time.Now().Add(-time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	taskCount := func() int {
		tasks, err := r1.st.Tasks(t.Context(), 500)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, task := range tasks {
			if strings.HasPrefix(task.IdempotencyKey, "sched:") {
				n++
			}
		}
		return n
	}
	waitTasks := func(want int) {
		waitFor(t, 10*time.Second, func() (bool, string) {
			n := taskCount()
			return n >= want, fmt.Sprintf("%d/%d scheduled tasks", n, want)
		})
		// Settle: give racing replicas every chance to double-fire, then
		// assert they didn't.
		time.Sleep(500 * time.Millisecond)
		if n := taskCount(); n != want {
			t.Fatalf("exactly-once violated: %d scheduled tasks, want %d", n, want)
		}
	}

	// Tick 1: both replicas' loops race for it.
	backdate()
	waitTasks(1)

	// Tick 2: kill replica 2 while passes are racing (crash-adjacent
	// shutdown), the surviving replica still fires the tick exactly once.
	backdate()
	r2.stop()
	waitTasks(2)

	// Tick 3: single replica steady state.
	backdate()
	waitTasks(3)

	// The schedule's bookkeeping points at a real queued task.
	var sched struct {
		Schedule struct {
			LastTaskID string `json:"last_task_id"`
			LastError  string `json:"last_error"`
		} `json:"schedule"`
	}
	doJSON(t, r1.hub.URL, "GET", "/api/v1/flows/tick/schedule", "", &sched)
	if sched.Schedule.LastTaskID == "" || sched.Schedule.LastError != "" {
		t.Fatalf("schedule bookkeeping = %+v", sched.Schedule)
	}
}
