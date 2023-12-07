package syncer

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.weni-ai/flows-field-syncer/models"
)

type SyncerPG struct {
	Conf    SyncerConf
	DB      *sqlx.DB
	FlowsDB *sqlx.DB
}

func NewSyncerPG(config SyncerConf) (*SyncerPG, error) {
	db, err := sqlx.Open("postgres", config.Service.Access["dsn"].(string))
	if err != nil {
		return nil, err
	}
	return &SyncerPG{
		DB:   db,
		Conf: config,
	}, nil
}

func (s *SyncerPG) GetLastModified() (time.Time, error) {
	log.Fatal("not implemented")
	return time.Time{}, errors.New("not implemented")
}

func (s *SyncerPG) GenerateSelectToSyncQuery() (string, error) {
	var columns []string
	table := s.Conf.Table

	columns = append(columns, table.RelationColumn) // include column fk

	for _, column := range table.Columns {
		columns = append(columns, column.Name)
	}
	columnList := strings.Join(columns, ", ")

	query := fmt.Sprintf("SELECT %s FROM %s", columnList, table.Name)
	return query, nil
}

func (s *SyncerPG) MakeQuery(ctx context.Context, query string) ([]map[string]any, error) {
	log.Println(query)
	rows, err := s.DB.QueryxContext(ctx, query)
	if err != nil {
		return nil, err
	}

	results := []map[string]any{}

	for rows.Next() {
		result := make(map[string]any)
		err := rows.MapScan(result)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func (s *SyncerPG) SyncContactFields(db *sqlx.DB) (int, error) {
	var updated int
	query, err := s.GenerateSelectToSyncQuery()
	if err != nil {
		return 0, errors.Wrap(err, "error generating query")
	}

	results, err := s.MakeQuery(context.TODO(), query)
	if err != nil {
		return 0, errors.Wrap(err, "error executing query")
	}

	// for each contact
	for _, r := range results {
		log.Println(r)
		// update fields
		for _, v := range s.Conf.Table.Columns {
			found := true
			// get contact field from flows
			field, err := models.GetContactFieldByOrgAndLabel(db, s.Conf.SyncRules.OrgID, v.FieldMapName)
			if err != nil {
				slog.Error(fmt.Sprintf("field could not be found in flows. field: %s", v.Name), "err", err)
				found = false
			}
			if found {
				// if field exists in flows, update that field in contact
				err := models.UpdateContactField(db, r[s.Conf.Table.RelationColumn].(string), field.UUID, r[v.Name])
				if err != nil {
					errMsg := fmt.Sprintf("field could not be updated: %v", field)
					slog.Error(errMsg, "err", err)
				}
			} else {
				// if field not exist, create it and create it in contact field column jsonb
				cf := models.NewContactField(v.FieldMapName, v.FieldMapName, s.Conf.SyncRules.OrgID, s.Conf.SyncRules.AdminID, s.Conf.SyncRules.AdminID)
				err := models.CreateContactField(db, cf)
				if err != nil {
					errMsg := fmt.Sprintf("error creating contact field: %v", err)
					slog.Error(errMsg, "err", err)
				} else {
					models.UpdateContactField(db, r[s.Conf.Table.RelationColumn].(string), cf.UUID, r[v.Name])
				}
			}
		}
		updated++
	}
	return updated, nil
}

func (s *SyncerPG) Close() error {
	return s.DB.Close()
}
