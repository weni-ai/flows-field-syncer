package models

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
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

func UpdateContactField(ctx context.Context, db *sqlx.DB, contactUUID string, fieldUUID string, fieldValue any) error {
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
	_, err := db.ExecContext(
		ctx,
		updateQuery,
		fmt.Sprintf(`{"%s"}`, fieldUUID),
		fmt.Sprintf(`{"text": "%s"}`, fieldValue),
		fmt.Sprintf(`{"%s": {"text": "%s"}}`, fieldUUID, fieldValue),
		contactUUID,
	)
	return err
}

func UpdateContactFieldByURN(ctx context.Context, db *sqlx.DB, pathURN string, orgID int64, fieldUUID string, fieldValue any) error {
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
		FROM public.contacts_contacturn
		WHERE public.contacts_contact.id = public.contacts_contacturn.contact_id
			AND public.contacts_contacturn.path = $4
			AND public.contacts_contacturn.org_id = $5;
	`
	_, err := db.ExecContext(
		ctx,
		updateQuery,
		fmt.Sprintf(`{"%s"}`, fieldUUID),
		fmt.Sprintf(`{"text": "%s"}`, fieldValue),
		fmt.Sprintf(`{"%s": {"text": "%s"}}`, fieldUUID, fieldValue),
		pathURN,
		orgID,
	)
	return err
}

func GetContactsURNPathByOrgID(ctx context.Context, db *sqlx.DB, orgID int64, scheme string) ([]string, error) {
	selectContactsSQL := `
	SELECT ccu.path
	FROM public.contacts_contacturn AS ccu
	JOIN public.contacts_contact AS cc ON ccu.contact_id = cc.id
	WHERE cc.org_id = $1
	AND cc.is_active = true
	AND ccu.scheme = $2
	`
	rows, err := db.QueryContext(ctx, selectContactsSQL, orgID, scheme)
	if err != nil {
		return nil, errors.Wrap(err, "Error querying contacts")
	}

	contacts := make([]string, 0)
	for rows.Next() {
		var path string
		err := rows.Scan(&path)
		if err != nil {
			return nil, err
		}
		contacts = append(contacts, path)
	}
	rows.Close()
	return contacts, nil
}
