# ADR-0050: A database node describes what it wants, not how to fetch it

Date: 2026-08-08

Status: **Designed; build deferred.** Extends ADR-0004 (DB sync/CDC as a
first-class workload), ADR-0018 (config-schema discovery), ADR-0024 (one node,
one verb) and ADR-0037 §sync (the watermark cursor built in M1.5). Does not
change the streaming contract: a structured query still lowers to one
parameterized `SELECT` streamed through the same cursor.

## Context

The `db` connector today has exactly one way in: a text box holding SQL.

```json
{"query": "SELECT id, name, updated_at FROM orders WHERE updated_at >= $1 ORDER BY updated_at"}
```

That is the right *primitive* and the wrong *default*. To fill it in, the
person building the flow must already know the table name, the column names,
their spelling and case, which column is monotonic, and the placeholder syntax
of the dialect. None of that is knowledge the integration is about; all of it
is knowledge the database itself already has and will happily report.

This is the specific thing people find miserable about Boomi's database
operations, and it is worth being precise about *why*, because "Boomi is hard"
is not a design input:

- The query is authored blind. There is no discovery in the builder, so the
  author alt-tabs to a SQL client, writes the query there, and pastes it in.
  The tool is a text field with extra steps.
- Nothing validates it until it runs against a live database, so the feedback
  loop for a typo is a failed execution rather than a red underline.
- The profile (the shape of the result) is maintained *separately* from the
  query that produces it, so the two drift, and the symptom of drift is a
  mapping that silently produces nulls.
- Incremental reads are a convention the author must re-implement per node,
  correctly, every time.

We have already inherited the fourth problem. `db sync` (M1.5) enforces its
correctness rules — `ORDER BY` on the cursor column, ascending, cursor column
selected — **by inspecting the SQL string**:

```go
if !strings.Contains(q, "order by") { … }
order := q[strings.LastIndex(q, "order by")+len("order by"):]
if strings.Contains(order, " desc") { … }
```

That works for the queries people actually write and is not a parser. `ORDER BY`
inside a subquery or a CTE, a column named `descending`, a comment containing
`order by` — each defeats it in a different direction, and the failure is
silent data loss, which is the worst category we have. Enforcing a structural
rule by grepping text is a stopgap, and it should be replaced by a shape in
which the rule cannot be expressed wrongly.

## Decision

Add a **structured query** alongside raw SQL: a declarative JSON spec that the
connector compiles to one parameterized statement.

```json
{
  "table": "public.orders",
  "select": ["id", "customer_id", "total", "updated_at"],
  "where": [
    {"column": "status", "op": "in", "value": ["open", "pending"]},
    {"column": "total", "op": ">=", "value": 100}
  ],
  "order_by": [{"column": "updated_at", "direction": "asc"}],
  "limit": 5000
}
```

Five things follow from that, and they are the entire justification:

**1. The studio can build the form, because the connector can describe the
database.** New discovery actions — `tables`, `columns` — turn the config form
into pickers over the real schema. This is the ADR-0018 mechanism used for
data rather than for config: the connector already self-describes, and a live
connection can describe the database the same way. No `information_schema`
knowledge reaches the flow author.

**2. Identifiers stop being string concatenation.** SQL cannot parameterize an
identifier, so a builder that accepts a table name is a builder that
concatenates it. Every identifier is therefore validated against the schema
the connection actually has, then quoted; a name that does not resolve to a
real table or column is a config error before a statement is built.
Values are *always* placeholders, never inlined — the parameterized-SQL-only
doctrine becomes structurally true rather than a rule authors follow.

**3. The incremental rules become structural.** `sync` over a structured query
does not inspect anything: it appends the watermark predicate and the ordering
itself, so "orders by the cursor column ascending" is a property of the
compiler, not a property of a string that happened to parse. The string
inspection in `validateSync` stays only for raw SQL, where it remains the only
option.

**4. The result shape is knowable without running it.** `select` names the
columns and discovery knows their types, so the builder can offer real field
names downstream instead of waiting for the first successful execution. This
is the Boomi profile-drift problem removed by construction rather than
managed: there is no second artefact to drift from.

**5. It composes with the verbs we already have.** `query`/`sync`/`upsert`
keep their current shape (ADR-0024: one node, a verb dropdown). A structured
query is a different way to *configure* the same verb, not a new verb.

### Raw SQL is not deprecated, and this is not an ORM

Raw SQL stays, permanently and without apology. Any real deployment eventually
needs a window function, a recursive CTE, a vendor hint, or a query a DBA
tuned and wants used verbatim. A builder that cannot express those is a
builder people escape from — the escape hatch is the thing that makes the
default safe to adopt.

Nor is this an ORM, despite the shape of the request. There is deliberately no
object mapping, no identity map, no lazy loading, no relation graph, no
migrations. Those exist to make a database look like an object model in a
host language, and the engine's record model already *is* the host
representation. What is being added is a **query specification** — the smallest
declarative surface that removes hand-written SQL for the common case:

| In scope | Out of scope |
|---|---|
| `table`, `select`, `where`, `order_by`, `limit`, `offset` | joins across tables (v1), subqueries, CTEs |
| aggregate + `group_by`/`having` | window functions |
| a fixed operator set over columns and literals | arbitrary SQL expressions |
| structured `upsert` targets | schema migration, DDL |

Joins are the one genuinely contested exclusion. They are excluded from v1
because a join builder is where a query spec starts becoming a query
*language*, and because the engine already has a spilling hash join
(ADR-0029): joining two streamed tables inside the flow is expressible today,
visible on the canvas, and does not put load on the customer's database. A
pushed-down join is a performance optimisation to add once there is evidence
it is needed, not a v1 requirement.

### Dialects

PostgreSQL is the only dialect the connector supports today, and the compiler
is written against it. The design keeps a dialect seam — placeholder syntax,
identifier quoting, `LIMIT`/`OFFSET` spelling, upsert form — because those are
the four things that differ and pretending otherwise makes the second dialect
a rewrite. No second dialect is built here; the seam exists so that adding one
is additive.

### Discovery is a capability, not an assumption

`tables`/`columns` need a live connection, which means the studio calls them
through a runner, which means they are subject to the same two-plane rule as
everything else: schema metadata is not payload, but it *is* customer data
about customer systems, so it travels the data plane (studio → runner →
database) and is never stored at the hub. A deployment whose runners are all
offline gets a config form that degrades to free text rather than one that
breaks — discovery failing must never make a node unconfigurable.

## Consequences

**Good.** The common case stops requiring SQL. The correctness rules that
currently rely on string inspection become structural. Identifier injection
stops being possible rather than being guarded against. The result shape is
known before first run.

**Costs, honestly.** A query compiler is a real surface with a real test
burden, and every operator added is a permanent compatibility commitment
(ADR-0047 §8 now diffs enum values, so the operator set is versioned the moment
it ships). Discovery adds two actions and a live-connection dependency to the
builder. And there will be a persistent pull toward "just one more clause"
until the spec becomes SQL with more syntax — the scope table above is the
defence, and it should be cited when the pull happens.

**Deferred deliberately.** Joins, window functions, a second dialect, and any
form of DDL. Each is additive to this design; none is needed to remove the
text box from the common case.

## Build order, when scheduled

1. `tables` + `columns` discovery actions, and the studio pickers over them —
   this alone removes the alt-tab, and is useful before any compiler exists.
2. The `select` compiler behind a `mode: structured` config, with raw SQL
   remaining the default until the compiler has proven itself.
3. `sync` over structured queries, replacing the string inspection for that
   path.
4. Structured `upsert` targets.
