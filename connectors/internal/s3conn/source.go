package s3conn

import (
	"context"
	"io"

	"github.com/aaron-au/shift/connectors/internal/decompress"
	"github.com/aaron-au/shift/connectors/internal/fileformat"
	"github.com/aaron-au/shift/engine/record"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// getSource streams a single object via GetObject, parsing the body into record
// batches with the configured format. The body is never buffered whole — the
// format reader wraps the response's io.ReadCloser directly.
type getSource struct {
	cfg     config
	body    interface{ Close() error }
	bounded *decompress.Reader // non-nil only when the object was compressed
	reader  fileformat.Reader
}

func (s *getSource) Open(ctx context.Context, cfg []byte) error {
	if err := parseConfig(cfg, &s.cfg); err != nil {
		return err
	}
	if err := s.cfg.requireKeyFormat(); err != nil {
		return err
	}
	api, err := newClient(&s.cfg)
	if err != nil {
		return err
	}
	out, err := api.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(s.cfg.Key),
	})
	if err != nil {
		return err
	}
	s.body = out.Body
	// An object's Content-Encoding is set by whoever wrote it to the bucket,
	// which in a shared or partner-fed bucket is not the flow's author. Before
	// this, a gzip-encoded object reached the record parser still compressed
	// and surfaced as `unexpected character '\x1f'` — gzip's magic number
	// reported as a data error (TC-020).
	body := io.Reader(out.Body)
	if out.ContentEncoding != nil && *out.ContentEncoding != "" {
		rd, err := decompress.Gzip(out.Body, decompress.Ratio(s.cfg.MaxDecompressionRatio), s.cfg.Key)
		if err != nil {
			return err
		}
		s.bounded = rd
		body = rd
	}
	rd, err := fileformat.NewReader(s.cfg.Format, body, fileformat.Options{RecordElement: s.cfg.RecordElement, Columns: s.cfg.Columns})
	if err != nil {
		return err
	}
	s.reader = rd
	return nil
}

func (s *getSource) Next(ctx context.Context) (*record.Batch, error) {
	b, err := s.reader.Next(ctx)
	if err == nil {
		return b, nil
	}
	// Consult the bound on every failure AND on EOF: a tripped ratio truncates
	// mid-record, so the format reader reports its own parse error — or a clean
	// EOF — and hides the real, size-shaped cause.
	if s.bounded != nil {
		if tripped := s.bounded.Tripped(); tripped != nil {
			return nil, tripped
		}
	}
	return b, err
}

func (s *getSource) Close() error {
	if s.body != nil {
		return s.body.Close()
	}
	return nil
}
