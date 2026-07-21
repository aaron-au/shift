// Command shift-consign signs and verifies connector artifacts
// (pkg/consign). Publishers run it at build/release time; the private
// key never leaves the publisher.
//
//	shift-consign keygen -out mykey
//	shift-consign sign   -key mykey.key -name http -version 1.0.0 [-os linux -arch amd64] artifact
//	shift-consign verify -pub mykey.pub -sig <base64|@file> -name http -version 1.0.0 [-os ... -arch ...] artifact
package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/aaron-au/shift/pkg/consign"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "keygen":
		err = keygen(os.Args[2:])
	case "sign":
		err = sign(os.Args[2:])
	case "publish":
		err = publish(os.Args[2:])
	case "verify":
		err = verify(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "shift-consign:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  shift-consign keygen  -out <prefix>
  shift-consign sign    -key <prefix>.key -name <n> -version <v> [-os <goos>] [-arch <goarch>]
                        [-descriptor <file> | -describe] <artifact>
  shift-consign publish -key <prefix>.key -name <n> -version <v> [-os <goos>] [-arch <goarch>]
                        -hub <url> -publisher-key <keyname> [-token <t>] [-descriptor <file> | -describe]
                        [-insecure] <artifact>
  shift-consign verify  -pub <prefix>.pub -sig <base64|@file> -name <n> -version <v> [-os <goos>] [-arch <goarch>] <artifact>

sign prints the digest + signature. publish signs and uploads to the hub
registry in one step (PUT /api/v1/connectors/{name}/versions/{version}).
-descriptor binds a descriptor blob (v2 signature, ADR-0018); -describe
extracts it by running '<artifact> describe' (host must match the artifact's
os/arch). -token defaults to $SHIFT_HUB_TOKEN.

Keys and signatures are single-line base64 files. The private key
(<prefix>.key, mode 0600) is the Ed25519 seed; keep it out of the hub.`)
}

func keygen(args []string) error {
	fs := flag.NewFlagSet("keygen", flag.ExitOnError)
	out := fs.String("out", "", "output file prefix (writes <prefix>.key and <prefix>.pub)")
	_ = fs.Parse(args)
	if *out == "" {
		return errors.New("keygen: -out is required")
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	if err := os.WriteFile(*out+".key", []byte(base64.StdEncoding.EncodeToString(priv.Seed())+"\n"), 0o600); err != nil {
		return err
	}
	if err := os.WriteFile(*out+".pub", []byte(base64.StdEncoding.EncodeToString(pub)+"\n"), 0o644); err != nil { //nolint:gosec // G306: public key is public
		return err
	}
	fmt.Printf("wrote %s.key (private, keep safe) and %s.pub\n", *out, *out)
	return nil
}

type manifestFlags struct {
	name, version, osName, arch string
}

func addManifestFlags(fs *flag.FlagSet) *manifestFlags {
	m := &manifestFlags{}
	fs.StringVar(&m.name, "name", "", "connector name")
	fs.StringVar(&m.version, "version", "", "artifact version")
	fs.StringVar(&m.osName, "os", runtime.GOOS, "target GOOS")
	fs.StringVar(&m.arch, "arch", runtime.GOARCH, "target GOARCH")
	return m
}

func (m *manifestFlags) manifest(artifact string) (consign.Manifest, error) {
	if m.name == "" || m.version == "" {
		return consign.Manifest{}, errors.New("-name and -version are required")
	}
	f, err := os.Open(artifact) //nolint:gosec // G304: user-supplied artifact path is this CLI's purpose
	if err != nil {
		return consign.Manifest{}, err
	}
	defer func() { _ = f.Close() }() // read-only
	digest, _, err := consign.HashReader(f)
	if err != nil {
		return consign.Manifest{}, err
	}
	return consign.Manifest{Name: m.name, Version: m.version, OS: m.osName, Arch: m.arch, Digest: digest}, nil
}

// descriptorFlags adds the two mutually-exclusive descriptor sources shared by
// sign and publish: a pre-extracted file, or on-the-fly `<artifact> describe`.
type descriptorFlags struct {
	file     string
	describe bool
}

func addDescriptorFlags(fs *flag.FlagSet) *descriptorFlags {
	d := &descriptorFlags{}
	fs.StringVar(&d.file, "descriptor", "", "descriptor blob file to bind (v2 signature, ADR-0018)")
	fs.BoolVar(&d.describe, "describe", false, "extract the descriptor by running '<artifact> describe'")
	return d
}

// load returns the descriptor bytes to bind (nil = v1, no descriptor). The
// bytes are used verbatim: hashed into the manifest's DescriptorDigest and
// uploaded as-is, so the hub can re-hash the stored blob and verify.
func (d *descriptorFlags) load(artifact string) ([]byte, error) {
	switch {
	case d.file != "" && d.describe:
		return nil, errors.New("use only one of -descriptor / -describe")
	case d.file != "":
		b, err := os.ReadFile(d.file) //nolint:gosec // G304: user-supplied descriptor path is this CLI's purpose
		if err != nil {
			return nil, err
		}
		return bytes.TrimRight(b, "\n"), nil
	case d.describe:
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, artifact, "describe").Output() //nolint:gosec // G204: signing the very binary the publisher built
		if err != nil {
			return nil, fmt.Errorf("running '%s describe': %w", artifact, err)
		}
		return bytes.TrimRight(out, "\n"), nil
	default:
		return nil, nil
	}
}

// bindDescriptor sets m.DescriptorDigest when descriptor bytes are present,
// bumping the signed message to the v2 form.
func bindDescriptor(m *consign.Manifest, descriptor []byte) {
	if len(descriptor) > 0 {
		m.DescriptorDigest = sha256.Sum256(descriptor)
	}
}

func sign(args []string) error {
	fs := flag.NewFlagSet("sign", flag.ExitOnError)
	keyFile := fs.String("key", "", "private key file (from keygen)")
	mf := addManifestFlags(fs)
	df := addDescriptorFlags(fs)
	_ = fs.Parse(args)
	if *keyFile == "" || fs.NArg() != 1 {
		return errors.New("sign: -key and exactly one artifact path are required")
	}
	seed, err := readB64File(*keyFile, ed25519.SeedSize)
	if err != nil {
		return fmt.Errorf("sign: %w", err)
	}
	m, err := mf.manifest(fs.Arg(0))
	if err != nil {
		return fmt.Errorf("sign: %w", err)
	}
	descriptor, err := df.load(fs.Arg(0))
	if err != nil {
		return fmt.Errorf("sign: %w", err)
	}
	bindDescriptor(&m, descriptor)
	sig := consign.Sign(ed25519.NewKeyFromSeed(seed), m)
	fmt.Printf("digest: sha256:%x\nsignature: %s\n", m.Digest, base64.StdEncoding.EncodeToString(sig))
	if len(descriptor) > 0 {
		fmt.Printf("descriptor: sha256:%x (%d bytes)\n", m.DescriptorDigest, len(descriptor))
	}
	return nil
}

// publish signs an artifact and uploads it to the hub registry in one step.
func publish(args []string) error {
	fs := flag.NewFlagSet("publish", flag.ExitOnError)
	keyFile := fs.String("key", "", "private key file (from keygen)")
	mf := addManifestFlags(fs)
	df := addDescriptorFlags(fs)
	hubURL := fs.String("hub", "", "hub base URL, e.g. https://hub:8400")
	token := fs.String("token", "", "admin bearer token (or $SHIFT_HUB_TOKEN)")
	pubKey := fs.String("publisher-key", "", "registered publisher key name (X-Shift-Publisher-Key)")
	insecure := fs.Bool("insecure", false, "skip TLS verification (dev only)")
	_ = fs.Parse(args)
	if *keyFile == "" || *hubURL == "" || *pubKey == "" || fs.NArg() != 1 {
		return errors.New("publish: -key, -hub, -publisher-key and exactly one artifact path are required")
	}
	tok := *token
	if tok == "" {
		tok = os.Getenv("SHIFT_HUB_TOKEN")
	}
	if tok == "" {
		return errors.New("publish: -token or $SHIFT_HUB_TOKEN is required")
	}
	seed, err := readB64File(*keyFile, ed25519.SeedSize)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	m, err := mf.manifest(fs.Arg(0))
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	descriptor, err := df.load(fs.Arg(0))
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	bindDescriptor(&m, descriptor)
	sig := consign.Sign(ed25519.NewKeyFromSeed(seed), m)

	artifact, err := os.ReadFile(fs.Arg(0)) //nolint:gosec // G304: user-supplied artifact path is this CLI's purpose
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	return uploadArtifact(strings.TrimRight(*hubURL, "/"), tok, *pubKey, m, sig, artifact, descriptor, *insecure)
}

// uploadArtifact PUTs the signed artifact to the hub registry.
func uploadArtifact(hubURL, token, pubKey string, m consign.Manifest, sig, artifact, descriptor []byte, insecure bool) error {
	url := fmt.Sprintf("%s/api/v1/connectors/%s/versions/%s?os=%s&arch=%s",
		hubURL, m.Name, m.Version, m.OS, m.Arch)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(artifact))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Shift-Publisher-Key", pubKey)
	req.Header.Set("X-Shift-Signature", base64.StdEncoding.EncodeToString(sig))
	req.Header.Set("Content-Type", "application/octet-stream")
	if len(descriptor) > 0 {
		req.Header.Set("X-Shift-Descriptor", base64.StdEncoding.EncodeToString(descriptor))
	}
	client := &http.Client{Timeout: 60 * time.Second}
	if insecure {
		// Dev affordance only: explicit opt-in for a self-signed hub cert.
		client.Transport = &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec // G402: gated behind an explicit -insecure dev flag
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("upload: hub returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	fmt.Printf("published %s@%s (%s/%s) → %s\n", m.Name, m.Version, m.OS, m.Arch, hubURL)
	return nil
}

func verify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	pubFile := fs.String("pub", "", "public key file (from keygen)")
	sigArg := fs.String("sig", "", "signature: base64 string, or @file containing one")
	mf := addManifestFlags(fs)
	_ = fs.Parse(args)
	if *pubFile == "" || *sigArg == "" || fs.NArg() != 1 {
		return errors.New("verify: -pub, -sig and exactly one artifact path are required")
	}
	pub, err := readB64File(*pubFile, ed25519.PublicKeySize)
	if err != nil {
		return fmt.Errorf("verify: %w", err)
	}
	sigB64 := *sigArg
	if strings.HasPrefix(sigB64, "@") {
		b, err := os.ReadFile(sigB64[1:])
		if err != nil {
			return fmt.Errorf("verify: %w", err)
		}
		sigB64 = strings.TrimSpace(string(b))
	}
	sig, err := base64.StdEncoding.DecodeString(sigB64)
	if err != nil {
		return fmt.Errorf("verify: decoding signature: %w", err)
	}
	m, err := mf.manifest(fs.Arg(0))
	if err != nil {
		return fmt.Errorf("verify: %w", err)
	}
	if err := consign.Verify(pub, m, sig); err != nil {
		return err
	}
	fmt.Println("OK")
	return nil
}

func readB64File(path string, wantLen int) ([]byte, error) {
	b, err := os.ReadFile(path) //nolint:gosec // G304: user-supplied key path is this CLI's purpose
	if err != nil {
		return nil, err
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(b)))
	if err != nil {
		return nil, fmt.Errorf("%s: not valid base64: %w", path, err)
	}
	if len(raw) != wantLen {
		return nil, fmt.Errorf("%s: decoded length %d, want %d", path, len(raw), wantLen)
	}
	return raw, nil
}
