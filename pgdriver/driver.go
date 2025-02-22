package pgdriver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/makasim/go-leaser"
)

type db interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type Driver struct {
	db     db
	dbName string
}

func New(db db, dbName string) *Driver {
	return &Driver{
		db:     db,
		dbName: dbName,
	}
}

func (d *Driver) Head(sinceRev, limit int64) ([]leaser.Lease, error) {
	qCtx, qCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer qCancel()
	q := `
SELECT resource, rev, owner, expires_at 
FROM 
	(
		SELECT xmin::text::bigint, resource, rev, owner, expires_at  
		FROM leases 
		WHERE rev > $1 
		ORDER BY "rev" ASC LIMIT $2
	) AS subquery
	CROSS JOIN (
		SELECT 
		split_part(pg_current_snapshot()::text, ':', 1)::bigint AS xmin, 
		split_part(pg_current_snapshot()::text, ':', 2)::bigint AS xmax
	) AS snapshot
WHERE subquery.xmin < snapshot.xmin OR subquery.xmin > snapshot.xmax
;
`

	rows, err := d.db.Query(qCtx, q, sinceRev, limit)
	if err != nil {
		return nil, fmt.Errorf("db: query: %w", err)
	}

	leases := make([]leaser.Lease, limit)
	var i int
	for rows.Next() {
		if err := rows.Scan(&leases[i].Resource, &leases[i].Rev, &leases[i].Owner, &leases[i].ExpireAt); err != nil {
			return nil, fmt.Errorf("rows: scan: %w", err)
		}
		i++
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("rows: %w", rows.Err())
	}

	return leases[:i], nil
}

func (d *Driver) Upsert(owner, resource string, dur time.Duration) error {
	if resource == "" {
		return fmt.Errorf("resource empty")
	}
	if owner == "" {
		return fmt.Errorf("owner empty")
	}
	durSec := int(dur.Seconds())
	if durSec <= 0 {
		return fmt.Errorf("duration must be not empty")
	}

	qCtx, qCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer qCancel()

	var rev int64

	err := d.db.QueryRow(qCtx, `
INSERT INTO leases (resource, owner, expires_at, rev)
VALUES ($1, $2, expires_at = NOW() + INTERVAL '1 second'*$3, nextval('leases_rev_seq'))
ON CONFLICT (resource) DO
	UPDATE SET owner = EXCLUDED.owner, expires_at = EXCLUDED.expires_at, rev = EXCLUDED.rev
	WHERE owner = $1 OR expires_at < NOW()
RETURNING rev
`, resource, owner, dur).Scan(&rev)
	if errors.Is(err, pgx.ErrNoRows) {
		return leaser.ErrAlreadyAcquired
	} else if err != nil {
		return err
	}

	return nil
}

func (d *Driver) Delete(owner, resource string) error {
	if owner == "" {
		return fmt.Errorf("owner must be not empty")
	}
	if resource == "" {
		return fmt.Errorf("resource must be not empty")
	}

	qCtx, qCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer qCancel()

	var rev int64

	err := d.db.QueryRow(qCtx, `
UPDATE leases 
SET owner = '', expires_at = 0, rev = nextval('leases_rev_seq')
WHERE owner = $1 AND resource = $2
RETURNING rev
`, owner, resource).Scan(&rev)
	if errors.Is(err, pgx.ErrNoRows) {
		// It's very bad sign.
		// It means that lease was already deleted by another owner, which means
		// that our in-memory state was out of sync with the database.
		return leaser.ErrNotAcquired
	} else if err != nil {
		return err
	}

	return nil
}
