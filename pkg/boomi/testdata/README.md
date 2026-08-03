# Synthetic Boomi fixtures

Every file here is **hand-authored**, not copied from a customer export.

That is a hard rule, not a convenience. Real Boomi exports carry customer
integration designs, folder hierarchies, author email addresses, and endpoint
names — none of which belong in this repository. The analyzer is deliberately
built to read an export from an arbitrary path so customer designs can stay
where they are allowed to live.

The fixtures mirror the *shape* of a real export (the `bns:Component` envelope,
the `process/shapes/shape` canvas, `dragpoints`, `encryptedValues`) with
invented names, so parser and report behavior can be asserted exactly.
