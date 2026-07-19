// Package engine is SHIFT's streaming data plane: the record/batch data
// model, pull-based pipelines, pooled buffer management, and
// spill-over-watermark handling.
//
// Design constraints (see docs/adr/0003 and 0004):
//   - Records are hierarchical and typed; no map[string]interface{} on the
//     hot path.
//   - Data moves as size-bounded record batches through pull-based streams;
//     batch boundaries are where backpressure, metrics, and spill decisions
//     happen.
//   - Memory is bounded by an explicit watermark, not by payload size; spill
//     goes to a single scratch store, never many small files.
//   - This module must remain free of network and database dependencies.
package engine
