// Package sdk is the connector SDK: the gRPC protocol contract, handshake
// and version negotiation, streaming source/sink interfaces, and the
// connector test kit.
//
// Connectors are standalone binaries spawned by the runner and spoken to
// over gRPC on unix domain sockets, streaming record batches (see
// docs/adr/0001). Interfaces here must be streaming end-to-end; a
// buffer-in/buffer-out surface is a design defect (v0 lesson).
package sdk
