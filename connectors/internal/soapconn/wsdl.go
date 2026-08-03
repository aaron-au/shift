package soapconn

import "errors"

// Operation is one callable SOAP operation discovered from a WSDL contract.
// It carries what the studio builder would need to pre-fill a `call` node:
// the SOAPAction to send, the port/binding it belongs to, and a request
// envelope skeleton with ${param} placeholders the author fills via the
// params map.
//
// This type and DiscoverOperations are a designed-but-deferred seed for
// ADR-0025 (WSDL-driven operation discovery: a plain function the publisher
// tooling or studio would call to turn a WSDL URL/document into a menu of
// ready-to-configure `call` operations, so the author picks an operation from
// a dropdown instead of hand-writing an envelope). Not yet wired.
type Operation struct {
	Name             string // operation name, e.g. "GetWeather"
	SOAPAction       string // value for the SOAPAction header
	Binding          string // binding/port the operation is exposed on
	InputMessage     string // WSDL input message name
	OutputMessage    string // WSDL output message name
	EnvelopeTemplate string // request skeleton with ${param} placeholders
}

// ErrWSDLNotImplemented marks WSDL discovery as not yet built (ADR-0025).
var ErrWSDLNotImplemented = errors.New("soap: WSDL operation discovery not implemented (ADR-0025 pending)")

// DiscoverOperations would parse a WSDL document and return the callable
// operations, each with a ready envelope skeleton. It is intentionally a
// stub: the shape is fixed here so the eventual ADR-0025 work (and the studio
// builder that consumes it) has a stable target, but the parser itself is
// deferred. Returns ErrWSDLNotImplemented.
func DiscoverOperations(_ []byte) ([]Operation, error) {
	return nil, ErrWSDLNotImplemented
}
