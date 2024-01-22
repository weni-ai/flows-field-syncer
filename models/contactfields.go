package models

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

const (
	ValueTypeText     = "T"
	ValueTypeNumber   = "N"
	ValueTypeDateTime = "D"
	ValueTypeState    = "S"
	ValueTypeDistrict = "I"
	ValueTypeWard     = "W"
)

type ContactField struct {
	ID           int64     `db:"id"`
	IsActive     bool      `db:"is_active"`
	CreatedOn    time.Time `db:"created_on"`
	ModifiedOn   time.Time `db:"modified_on"`
	UUID         string    `db:"uuid"`
	Label        string    `db:"label"`
	Key          string    `db:"key"`
	FieldType    string    `db:"field_type"`
	ValueType    string    `db:"value_type"`
	ShowInTable  bool      `db:"show_in_table"`
	Priority     int       `db:"priority"`
	CreatedByID  int64     `db:"created_by_id"`
	ModifiedByID int64     `db:"modified_by_id"`
	OrgID        int64     `db:"org_id"`
}

func NewContactField(label, key, valueType string, orgID, createdByID, modifiedByID int64) ContactField {
	return ContactField{
		IsActive:     true,
		CreatedOn:    time.Now(),
		ModifiedOn:   time.Now(),
		UUID:         uuid.New().String(),
		Label:        label,
		Key:          key,
		FieldType:    "U",
		ValueType:    valueType,
		ShowInTable:  false,
		Priority:     0,
		CreatedByID:  createdByID,
		ModifiedByID: modifiedByID,
		OrgID:        orgID,
	}
}

func GetContactFieldByOrgAndKey(ctx context.Context, db *sqlx.DB, orgID int64, key string) (ContactField, error) {
	var contactField ContactField

	query := `
		SELECT
			id,
			is_active,
			created_on,
			modified_on,
			uuid,
			label,
			key,
			field_type,
			value_type,
			show_in_table,
			priority,
			created_by_id,
			modified_by_id,
			org_id
		FROM
			public.contacts_contactfield
		WHERE
			org_id = $1 AND
			key = $2
	`

	err := db.GetContext(ctx, &contactField, query, orgID, key)
	if err != nil {
		return ContactField{}, fmt.Errorf("error getting contact field: %v", err)
	}

	return contactField, nil
}

func CreateContactField(ctx context.Context, db *sqlx.DB, contactField ContactField) error {
	query := `
		INSERT INTO public.contacts_contactfield (
			is_active,
			created_on,
			modified_on,
			uuid,
			label,
			key,
			field_type,
			value_type,
			show_in_table,
			priority,
			created_by_id,
			modified_by_id,
			org_id
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
		) RETURNING id
	`

	err := db.QueryRowContext(
		ctx,
		query,
		contactField.IsActive,
		contactField.CreatedOn,
		contactField.ModifiedOn,
		contactField.UUID,
		contactField.Label,
		contactField.Key,
		contactField.FieldType,
		contactField.ValueType,
		contactField.ShowInTable,
		contactField.Priority,
		contactField.CreatedByID,
		contactField.ModifiedByID,
		contactField.OrgID,
	).Scan(&contactField.ID)

	if err != nil {
		return fmt.Errorf("error creating contact field: %v", err)
	}

	return nil
}

func ScanValueType(value interface{}) string {
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return ValueTypeNumber
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return ValueTypeNumber
	case reflect.Float32, reflect.Float64:
		return ValueTypeNumber
	case reflect.String:
		return ValueTypeText
	case reflect.Struct:
		if v.Type() == reflect.TypeOf(time.Time{}) {
			return ValueTypeDateTime
		} else {
			return ValueTypeText
		}
	default:
		return ValueTypeText
	}
}
