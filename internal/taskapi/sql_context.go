package taskapi

import (
	"context"
	"database/sql"
)

// SQL transaction bindings are server-local context values. Each binding is
// keyed by the exact original *sql.DB; a callback cannot borrow a foreign
// store's transaction or supply this authority in a transport request.
type sqlTransactionKey struct{}
type sqlTransactionBinding struct {
	db       *sql.DB
	tx       *sql.Tx
	previous *sqlTransactionBinding
}

func BindSQLTransaction(ctx context.Context, db *sql.DB, tx *sql.Tx) context.Context {
	previous, _ := ctx.Value(sqlTransactionKey{}).(*sqlTransactionBinding)
	return context.WithValue(ctx, sqlTransactionKey{}, &sqlTransactionBinding{db, tx, previous})
}
func BoundSQLTransaction(ctx context.Context, db *sql.DB) *sql.Tx {
	binding, _ := ctx.Value(sqlTransactionKey{}).(*sqlTransactionBinding)
	for ; binding != nil; binding = binding.previous {
		if binding.db == db {
			return binding.tx
		}
	}
	return nil
}

type SQLReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func ReadSQL(ctx context.Context, db *sql.DB) SQLReader {
	if tx := BoundSQLTransaction(ctx, db); tx != nil {
		return tx
	}
	return db
}
