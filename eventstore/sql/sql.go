package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/hallgren/eventsourcing/core"
)

// SQL event store handler
type SQL struct {
	db *sql.DB
}

// Open connection to database
func Open(db *sql.DB) *SQL {
	return &SQL{
		db: db,
	}
}

// Close the connection
func (s *SQL) Close() {
	s.db.Close()
}

// Save persists events to the database
func (s *SQL) Save(events []core.Event) error {
	// If no event return no error
	if len(events) == 0 {
		return nil
	}
	aggregateID := events[0].AggregateID
	aggregateType := events[0].AggregateType

	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return errors.New(fmt.Sprintf("could not start a write transaction, %v", err))
	}
	defer tx.Rollback()

	var currentVersion core.Version
	var version int
	selectStm := `Select version from events where id=? and type=? order by version desc limit 1`
	err = tx.QueryRow(selectStm, aggregateID, aggregateType).Scan(&version)
	if err != nil && err != sql.ErrNoRows {
		return err
	} else if err == sql.ErrNoRows {
		// if no events are saved before set the current version to zero
		currentVersion = core.Version(0)
	} else {
		// set the current version to the last event stored
		currentVersion = core.Version(version)
	}

	// Make sure no other has saved event to the same aggregate concurrently
	if core.Version(currentVersion)+1 != events[0].Version {
		return core.ErrConcurrency
	}

	var lastInsertedID int64
	insert := `Insert into events (id, version, reason, type, timestamp, data, metadata) values ($1, $2, $3, $4, $5, $6, $7)`
	for i, event := range events {
		res, err := tx.Exec(insert, event.AggregateID, event.Version, event.Reason, event.AggregateType, event.Timestamp.Format(time.RFC3339), event.Data, event.Metadata)
		if err != nil {
			return err
		}
		lastInsertedID, err = res.LastInsertId()
		if err != nil {
			return err
		}
		// override the event in the slice exposing the GlobalVersion to the caller
		events[i].GlobalVersion = core.Version(lastInsertedID)
	}
	return tx.Commit()
}

// Get the events from database
func (s *SQL) Get(ctx context.Context, id string, aggregateType string, afterVersion core.Version) (core.Iterator, error) {
	selectStm := `Select seq, id, version, reason, type, timestamp, data, metadata from events where id=? and type=? and version>? order by version asc`
	rows, err := s.db.QueryContext(ctx, selectStm, id, aggregateType, afterVersion)
	if err != nil {
		return nil, err
	}
	return &iterator{rows: rows}, nil
}

// GlobalEvents return count events in order globally from the start posistion
func (s *SQL) GlobalEvents(start, count uint64) ([]core.Event, error) {
	selectStm := `Select seq, id, version, reason, type, timestamp, data, metadata from events where seq >= ? order by seq asc LIMIT ?`
	rows, err := s.db.Query(selectStm, start, count)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return s.eventsFromRows(rows)
}

func (s *SQL) eventsFromRows(rows *sql.Rows) ([]core.Event, error) {
	var events []core.Event
	for rows.Next() {
		var globalVersion core.Version
		var version core.Version
		var id, reason, typ, timestamp string
		var data, metadata []byte
		if err := rows.Scan(&globalVersion, &id, &version, &reason, &typ, &timestamp, &data, &metadata); err != nil {
			return nil, err
		}

		t, err := time.Parse(time.RFC3339, timestamp)
		if err != nil {
			return nil, err
		}

		events = append(events, core.Event{
			AggregateID:   id,
			Version:       version,
			GlobalVersion: globalVersion,
			AggregateType: typ,
			Timestamp:     t,
			Data:          data,
			Metadata:      metadata,
			Reason:        reason,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}
