package leaseloop

import (
	"reflect"
	"testing"

	"github.com/aaron-au/shift/runner/internal/hubclient"
	"github.com/aaron-au/shift/runner/internal/task"
)

// resultOf is the only place a completed task becomes the hub's durable
// execution report. Every field it forgets is a metric that silently never
// arrives, so this pins the whole conversion rather than a sample of it.
func TestResultOfCarriesEveryField(t *testing.T) {
	lt := task.Task{
		RecordsIn:     100,
		RecordsOut:    90,
		SinkConfirmed: 90,
		Stopped:       true,
		StopStep:      "stop1",
		Phases: task.Phases{
			AdmissionMS: 1.5,
			BindMS:      2.5,
			RunMS:       3.5,
			TotalMS:     7.5,
		},
		Ops: []task.OpStat{
			{
				Name: "filter", StepID: "f1",
				RecordsIn: 100, RecordsOut: 90, Seconds: 0.25,
				Batches: 4, WallSeconds: 0.75, Bytes: 4096,
			},
			{Name: "sink", StepID: "s1", RecordsIn: 90, RecordsOut: 90},
		},
	}

	got := resultOf(lt, "local-1")

	want := hubclient.Result{
		RecordsIn:     100,
		RecordsOut:    90,
		SinkConfirmed: 90,
		RunnerTaskID:  "local-1",
		Stopped:       true,
		StopStep:      "stop1",
		Phases:        hubclient.Phases{AdmissionMS: 1.5, BindMS: 2.5, RunMS: 3.5, TotalMS: 7.5},
		Ops: []hubclient.OpStat{
			{
				Name: "filter", StepID: "f1",
				RecordsIn: 100, RecordsOut: 90, Seconds: 0.25,
				Batches: 4, WallSeconds: 0.75, Bytes: 4096,
			},
			{Name: "sink", StepID: "s1", RecordsIn: 90, RecordsOut: 90},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("result:\n got %+v\nwant %+v", got, want)
	}
}

// A task that never ran must not report zeroed phases as if they were
// measured: omitzero keeps the block off the wire entirely (see task.Task).
func TestResultOfZeroTaskCarriesNoPhases(t *testing.T) {
	got := resultOf(task.Task{}, "local-2")
	if got.Phases != (hubclient.Phases{}) {
		t.Errorf("phases: got %+v, want zero", got.Phases)
	}
	if got.Ops != nil {
		t.Errorf("ops: got %v, want nil", got.Ops)
	}
}
