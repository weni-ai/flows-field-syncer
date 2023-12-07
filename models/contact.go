package models

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

type Contact struct {
	ID         int64          `db:"id"`
	UUID       string         `db:"uuid"`
	Name       string         `db:"name"`
	Fields     map[string]any `db:"fields"`
	CreatedOn  time.Time      `db:"created_on"`
	ModifiedOn time.Time      `db:"modified_on"`
	LastSeenOn *time.Time     `db:"last_seen_on"`
}

func UpdateContactField(db *sqlx.DB, contactUUID string, fieldUUID string, fieldValue any) error {
	updateQuery := `
		UPDATE public.contacts_contact
		SET fields = COALESCE(
				JSONB_SET(
						coalesce(fields, '{}'),
						$1,
						$2,
						true
				),
				COALESCE(fields, '{}') || $3
		)
		WHERE uuid = $4;
	`
	_, err := db.Exec(
		updateQuery,
		fmt.Sprintf(`{"%s"}`, fieldUUID),
		fmt.Sprintf(`{"text": "%s"}`, fieldValue),
		fmt.Sprintf(`{"%s": {"text": "%s"}}`, fieldUUID, fieldValue),
		contactUUID,
	)
	return err
}
