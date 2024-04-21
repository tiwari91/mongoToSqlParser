package domain

import (
	"database/sql"
	"time"
)

func PositionExists(db *sql.DB, position int) (bool, error) {
	var exists bool
	err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM bookmark WHERE position = $1)", position).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func SavePosition(db *sql.DB, position int) error {
	_, err := db.Exec("INSERT INTO bookmark (position, timestamp) VALUES ($1, $2)", position, time.Now())

	return err
}
