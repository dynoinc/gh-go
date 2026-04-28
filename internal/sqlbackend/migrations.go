package sqlbackend

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// runMigrations applies all pending migrations to the database
func runMigrations(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER NOT NULL PRIMARY KEY, dirty BOOLEAN NOT NULL)`); err != nil {
		return fmt.Errorf("creating schema migrations table: %w", err)
	}

	currentVersion, dirty, err := currentMigrationVersion(db)
	if err != nil {
		return err
	}
	if dirty {
		return errors.New("schema migrations table is dirty")
	}

	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("reading migrations: %w", err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".up.sql") {
			continue
		}

		version, err := migrationVersion(name)
		if err != nil {
			return err
		}
		if version <= currentVersion {
			continue
		}

		contents, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("reading migration %s: %w", name, err)
		}
		if err := recordMigrationVersion(db, version, true); err != nil {
			return err
		}
		if _, err := db.Exec(string(contents)); err != nil {
			return fmt.Errorf("applying migration %s: %w", name, err)
		}
		if err := recordMigrationVersion(db, version, false); err != nil {
			return err
		}
		currentVersion = version
	}

	return nil
}

func currentMigrationVersion(db *sql.DB) (int, bool, error) {
	var version int
	var dirty bool
	err := db.QueryRow(`SELECT version, dirty FROM schema_migrations LIMIT 1`).Scan(&version, &dirty)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("reading schema migration version: %w", err)
	}
	return version, dirty, nil
}

func recordMigrationVersion(db *sql.DB, version int, dirty bool) error {
	if _, err := db.Exec(`DELETE FROM schema_migrations`); err != nil {
		return fmt.Errorf("clearing schema migration version: %w", err)
	}
	if _, err := db.Exec(`INSERT INTO schema_migrations (version, dirty) VALUES (?, ?)`, version, dirty); err != nil {
		return fmt.Errorf("recording migration version %d: %w", version, err)
	}
	return nil
}

func migrationVersion(name string) (int, error) {
	prefix, _, ok := strings.Cut(name, "_")
	if !ok {
		return 0, fmt.Errorf("invalid migration filename %q", name)
	}
	version, err := strconv.Atoi(prefix)
	if err != nil {
		return 0, fmt.Errorf("invalid migration version in %q: %w", name, err)
	}
	return version, nil
}
