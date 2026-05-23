package tickr

import (
	"context"
	"fmt"
)

// Migrator is the schema-management façade. It delegates to the configured
// Storage adapter, which embeds its own migrations.
type Migrator struct {
	s Storage
}

// NewMigrator constructs a Migrator that drives the given Storage.
func NewMigrator(s Storage) (*Migrator, error) {
	if s == nil {
		return nil, fmt.Errorf("tickr: NewMigrator requires Storage")
	}
	return &Migrator{s: s}, nil
}

// Up applies all pending migrations.
func (m *Migrator) Up(ctx context.Context) error {
	return m.s.ApplyMigrations(ctx)
}
