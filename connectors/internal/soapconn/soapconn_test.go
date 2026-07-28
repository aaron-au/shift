package soapconn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

// --- canned SOAP responses ---------------------------------------------------

const successBody = `<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetWeatherResponse xmlns="urn:weather">
      <City>Melbourne</City>
      <TempC unit="celsius">17</TempC>
      <Forecast>
        <Day>Mon</Day>
        <Day>Tue</Day>
        <Day>Wed</Day>
      </Forecast>
    </GetWeatherResponse>
  </soap:Body>
</soap:Envelope>`

const fault11Body = `<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
  <soapenv:Body>
    <soapenv:Fault>
      <faultcode>soapenv:Server</faultcode>
      <faultstring>Unknown city code</faultstring>
    </soapenv:Fault>
  </soapenv:Body>
</soapenv:Envelope>`

const fault12Body = `<?xml version="1.0" encoding="utf-8"?>
<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
  <env:Body>
    <env:Fault>
      <env:Code><env:Value>env:Sender</env:Value></env:Code>
      <env:Reason><env:Text xml:lang="en">Bad request payload</env:Text></env:Reason>
    </env:Fault>
  </env:Body>
</env:Envelope>`

// soapServer serves the given body with the given HTTP status.
func soapServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/xml; charset=utf-8")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func callConfig(endpoint string, params map[string]string) []byte {
	tmpl := `<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><GetWeather><City>${city}</City></GetWeather></soap:Body></soap:Envelope>`
	p := `{}`
	if len(params) > 0 {
		parts := make([]string, 0, len(params))
		for k, v := range params {
			parts = append(parts, fmt.Sprintf("%q:%q", k, v))
		}
		p = "{" + strings.Join(parts, ",") + "}"
	}
	return fmt.Appendf(nil, `{"endpoint":%q,"soap_action":"urn:GetWeather","envelope_template":%q,"params":%s,"allow_local":true}`,
		endpoint, tmpl, p)
}

// --- tests -------------------------------------------------------------------

func TestCallParsesSuccessResponse(t *testing.T) {
	var gotAction, gotCT, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAction = r.Header.Get("SOAPAction")
		gotCT = r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Header().Set("Content-Type", "text/xml; charset=utf-8")
		_, _ = io.WriteString(w, successBody)
	}))
	t.Cleanup(srv.Close)

	s := &callSource{}
	ctx := context.Background()
	if err := s.Open(ctx, callConfig(srv.URL, map[string]string{"city": "Melbourne"})); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()

	b, err := s.Next(ctx)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	recs := b.Records()
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1 (the response element)", len(recs))
	}
	rec := recs[0] // GetWeatherResponse map

	city, ok := field(rec, "City")
	if !ok || city.String() != "Melbourne" {
		t.Fatalf("City = %v (ok=%v), want Melbourne", city.String(), ok)
	}
	// Element with an attribute + text becomes a map: {"@unit":..,"#text":..}.
	temp, ok := field(rec, "TempC")
	if !ok || temp.Kind() != record.KindMap {
		t.Fatalf("TempC kind = %v, want map", temp.Kind())
	}
	if unit, _ := field(temp, "@unit"); unit.String() != "celsius" {
		t.Fatalf("TempC @unit = %q, want celsius", unit.String())
	}
	if text, _ := field(temp, "#text"); text.String() != "17" {
		t.Fatalf("TempC #text = %q, want 17", text.String())
	}
	// Repeated <Day> children collapse into a list under Forecast.
	fc, ok := field(rec, "Forecast")
	if !ok {
		t.Fatal("Forecast missing")
	}
	days, ok := field(fc, "Day")
	if !ok || days.Kind() != record.KindList || days.Len() != 3 {
		t.Fatalf("Forecast.Day = %v (kind %v), want a 3-element list", days, days.Kind())
	}
	if days.Index(0).String() != "Mon" || days.Index(2).String() != "Wed" {
		t.Fatalf("days = [%s..%s], want Mon..Wed", days.Index(0).String(), days.Index(2).String())
	}

	// Second Next → EOF.
	if _, err := s.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("second Next = %v, want EOF", err)
	}

	// Request shape: quoted SOAPAction, SOAP 1.1 content-type, escaped param
	// substituted into the envelope.
	if gotAction != `"urn:GetWeather"` {
		t.Fatalf("SOAPAction = %q, want quoted urn:GetWeather", gotAction)
	}
	if !strings.HasPrefix(gotCT, "text/xml") {
		t.Fatalf("Content-Type = %q, want text/xml", gotCT)
	}
	if !strings.Contains(gotBody, "<City>Melbourne</City>") {
		t.Fatalf("request body did not substitute the param: %s", gotBody)
	}
}

func TestCallSurfacesSOAP11Fault(t *testing.T) {
	// SOAP faults arrive with HTTP 500 — the connector must still parse the
	// body and turn the Fault into an error, not a bare status error.
	srv := soapServer(t, http.StatusInternalServerError, fault11Body)
	s := &callSource{}
	ctx := context.Background()
	if err := s.Open(ctx, callConfig(srv.URL, nil)); err != nil {
		t.Fatalf("open: %v", err)
	}
	_, err := s.Next(ctx)
	if err == nil {
		t.Fatal("expected a fault error, got nil")
	}
	if !strings.Contains(err.Error(), "fault") || !strings.Contains(err.Error(), "Unknown city code") {
		t.Fatalf("fault error = %v, want it to mention the faultstring", err)
	}
}

func TestCallSurfacesSOAP12Fault(t *testing.T) {
	srv := soapServer(t, http.StatusInternalServerError, fault12Body)
	s := &callSource{}
	ctx := context.Background()
	if err := s.Open(ctx, callConfig(srv.URL, nil)); err != nil {
		t.Fatalf("open: %v", err)
	}
	_, err := s.Next(ctx)
	if err == nil || !strings.Contains(err.Error(), "Bad request payload") {
		t.Fatalf("SOAP 1.2 fault error = %v, want the Reason text", err)
	}
}

func TestCallNon2xxWithoutFault(t *testing.T) {
	srv := soapServer(t, http.StatusBadGateway, "upstream exploded")
	s := &callSource{}
	ctx := context.Background()
	if err := s.Open(ctx, callConfig(srv.URL, nil)); err != nil {
		t.Fatalf("open: %v", err)
	}
	_, err := s.Next(ctx)
	if err == nil || !strings.Contains(err.Error(), "502") {
		t.Fatalf("error = %v, want it to mention status 502", err)
	}
}

func TestSSRFGuardRefusesLoopbackByDefault(t *testing.T) {
	// allow_local omitted → the guard must refuse the loopback httptest server.
	srv := soapServer(t, http.StatusOK, successBody)
	tmpl := `<x/>`
	cfg := fmt.Appendf(nil, `{"endpoint":%q,"envelope_template":%q}`, srv.URL, tmpl)
	s := &callSource{}
	ctx := context.Background()
	if err := s.Open(ctx, cfg); err != nil {
		t.Fatalf("open: %v", err)
	}
	_, err := s.Next(ctx)
	if err == nil || !strings.Contains(err.Error(), "refusing") {
		t.Fatalf("error = %v, want an SSRF-guard refusal", err)
	}
}

func TestConfigValidation(t *testing.T) {
	cases := map[string]string{
		"missing endpoint": `{"envelope_template":"<x/>","allow_local":true}`,
		"missing template": `{"endpoint":"http://h/svc","allow_local":true}`,
		"bad soap version": `{"endpoint":"http://h/svc","envelope_template":"<x/>","soap_version":"3.0"}`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			var c config
			if err := parseConfig([]byte(raw), &c); err == nil {
				t.Fatalf("%s: expected a validation error", name)
			}
		})
	}
	t.Run("defaults applied", func(t *testing.T) {
		var c config
		if err := parseConfig([]byte(`{"endpoint":"http://h/svc","envelope_template":"<x/>"}`), &c); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.TimeoutSeconds != 60 || c.MaxResponseBytes != defaultMaxResponseBytes {
			t.Fatalf("defaults not applied: timeout=%d max=%d", c.TimeoutSeconds, c.MaxResponseBytes)
		}
	})
}

func TestRenderEnvelope(t *testing.T) {
	tmpl := `<a>${name}</a><b>$id</b><c>$missing</c>`
	got, err := renderEnvelope(tmpl, map[string]string{"name": "A&B <ok>", "id": "42"})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	// name value is XML-escaped; id substituted; missing left verbatim.
	if !strings.Contains(got, "<a>A&amp;B &lt;ok&gt;</a>") {
		t.Fatalf("param not escaped: %q", got)
	}
	if !strings.Contains(got, "<b>42</b>") {
		t.Fatalf("bare $id not substituted: %q", got)
	}
	if !strings.Contains(got, "<c>$missing</c>") {
		t.Fatalf("unknown placeholder should stay verbatim: %q", got)
	}
}

func TestContentTypeByVersion(t *testing.T) {
	c := config{SOAPVersion: "1.2"}
	if !strings.HasPrefix(c.contentType(), "application/soap+xml") {
		t.Fatalf("1.2 content-type = %q", c.contentType())
	}
	c = config{}
	if !strings.HasPrefix(c.contentType(), "text/xml") {
		t.Fatalf("default content-type = %q", c.contentType())
	}
}

func TestConnectorAndDescriptor(t *testing.T) {
	c := Connector()
	if c.Name != "soap" {
		t.Fatalf("name = %q", c.Name)
	}
	if _, ok := c.Sources["call"]; !ok {
		t.Fatal("call source not registered")
	}
	if len(c.Sinks) != 0 {
		t.Fatalf("soap has no sinks, got %d", len(c.Sinks))
	}
	if _, ok := c.Schemas["call"]; !ok {
		t.Fatal("call schema missing")
	}
}

func TestDiscoverOperationsStub(t *testing.T) {
	_, err := DiscoverOperations([]byte(`<wsdl/>`))
	if !errors.Is(err, ErrWSDLNotImplemented) {
		t.Fatalf("stub error = %v, want ErrWSDLNotImplemented", err)
	}
}

// field is a test helper for record map lookups.
func field(v record.Value, name string) (record.Value, bool) { return v.Field(name) }
