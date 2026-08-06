# Vendored JSON Schema conformance fixtures

These files are copied verbatim from
[json-schema-org/JSON-Schema-Test-Suite](https://github.com/json-schema-org/JSON-Schema-Test-Suite)
(`tests/draft2020-12/`), which is published under the MIT licence. They are
test data only — nothing here is compiled into a shipped binary.

Refresh with `./scripts/fetch-schema-suite.sh`, then review the diff: a change
in the corpus is a change in what the specification requires, and it should be
read rather than merged blind.

## Why they are here

ADR-0042 §4c-i: **a keyword is enabled only if it passes its section of this
suite.** Hand-written tests check the cases the author thought of, and the
cases an author does not think of are exactly where a validator quietly
disagrees with the specification.

That is not theoretical. On the first run this corpus found three real bugs in
`engine/schema` that the hand-written tests had missed:

1. **`$ref` was treated as replacing its siblings.** In 2020-12 it does not:
   `{"$ref": "…", "maxItems": 2}` must enforce both. Every sibling assertion
   was being silently dropped — the precise failure mode this package exists
   to prevent.
2. **Leap seconds were accepted at any hour.** `22:59:60Z` is not a time that
   has ever existed; a leap second occurs at 23:59:60 UTC and nowhere else.
3. **`format: email` disagreed with the grammar in both directions** —
   rejecting legal quoted local parts and address literals, while accepting
   `.test@x.com`, `test.@x.com` and `te..st@x.com`, which are not legal. It now
   implements RFC 5321's mailbox grammar and parses address literals rather
   than merely checking for brackets.

## What is not vendored

`tests/draft2020-12/format.json` tests `format` as an **annotation**, where a
conformant validator accepts `"not-a-date"`. This subset asserts formats
instead, so those cases would fail by design. The assertion cases under
`optional/format/` are vendored in their place.
