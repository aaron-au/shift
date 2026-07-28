package dbconn

import (
	"context"
	"database/sql"
	"fmt"
)

// Column describes one column of a table.
type Column struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// Table describes one base table and its columns, in ordinal order.
type Table struct {
	Schema  string   `json:"schema"`
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
}

// discoverSchema reads information_schema.tables + columns and returns the base
// tables (user schemas only) with their columns. It is a plain function — a seed
// for connector introspection (ADR-0025), NOT the RPC — so the introspection
// work can grow around a small, tested primitive.
func discoverSchema(ctx context.Context, db *sql.DB) ([]Table, error) {
	const q = `
SELECT c.table_schema, c.table_name, c.column_name, c.data_type, c.is_nullable
FROM information_schema.columns c
JOIN information_schema.tables t
  ON t.table_schema = c.table_schema
 AND t.table_name = c.table_name
 AND t.table_type = 'BASE TABLE'
WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY c.table_schema, c.table_name, c.ordinal_position`

	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("db: discover schema: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tables []Table
	var cur *Table
	for rows.Next() {
		var schema, name, col, dataType, isNullable string
		if err := rows.Scan(&schema, &name, &col, &dataType, &isNullable); err != nil {
			return nil, fmt.Errorf("db: discover scan: %w", err)
		}
		if cur == nil || cur.Schema != schema || cur.Name != name {
			tables = append(tables, Table{Schema: schema, Name: name})
			cur = &tables[len(tables)-1]
		}
		cur.Columns = append(cur.Columns, Column{
			Name:     col,
			Type:     dataType,
			Nullable: isNullable == "YES",
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("db: discover read: %w", err)
	}
	return tables, nil
}
