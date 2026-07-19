// Command shift-consign signs and verifies connector artifacts
// (pkg/consign). Publishers run it at build/release time; the private
// key never leaves the publisher.
//
//	shift-consign keygen -out mykey
//	shift-consign sign   -key mykey.key -name http -version 1.0.0 [-os linux -arch amd64] artifact
//	shift-consign verify -pub mykey.pub -sig <base64|@file> -name http -version 1.0.0 [-os ... -arch ...] artifact
package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"

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
  shift-consign keygen -out <prefix>
  shift-consign sign   -key <prefix>.key -name <n> -version <v> [-os <goos>] [-arch <goarch>] <artifact>
  shift-consign verify -pub <prefix>.pub -sig <base64|@file> -name <n> -version <v> [-os <goos>] [-arch <goarch>] <artifact>

Keys and signatures are single-line base64 files. The private key
(<prefix>.key, mode 0600) is the Ed25519 seed; keep it out of the hub.`)
}

func keygen(args []string) error {
	fs := flag.NewFlagSet("keygen", flag.ExitOnError)
	out := fs.String("out", "", "output file prefix (writes <prefix>.key and <prefix>.pub)")
	_ = fs.Parse(args)
	if *out == "" {
		return fmt.Errorf("keygen: -out is required")
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
		return consign.Manifest{}, fmt.Errorf("-name and -version are required")
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

func sign(args []string) error {
	fs := flag.NewFlagSet("sign", flag.ExitOnError)
	keyFile := fs.String("key", "", "private key file (from keygen)")
	mf := addManifestFlags(fs)
	_ = fs.Parse(args)
	if *keyFile == "" || fs.NArg() != 1 {
		return fmt.Errorf("sign: -key and exactly one artifact path are required")
	}
	seed, err := readB64File(*keyFile, ed25519.SeedSize)
	if err != nil {
		return fmt.Errorf("sign: %w", err)
	}
	m, err := mf.manifest(fs.Arg(0))
	if err != nil {
		return fmt.Errorf("sign: %w", err)
	}
	sig := consign.Sign(ed25519.NewKeyFromSeed(seed), m)
	fmt.Printf("digest: sha256:%x\nsignature: %s\n", m.Digest, base64.StdEncoding.EncodeToString(sig))
	return nil
}

func verify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	pubFile := fs.String("pub", "", "public key file (from keygen)")
	sigArg := fs.String("sig", "", "signature: base64 string, or @file containing one")
	mf := addManifestFlags(fs)
	_ = fs.Parse(args)
	if *pubFile == "" || *sigArg == "" || fs.NArg() != 1 {
		return fmt.Errorf("verify: -pub, -sig and exactly one artifact path are required")
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
