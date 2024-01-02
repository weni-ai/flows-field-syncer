package syncer

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.weni-ai/flows-field-syncer/models"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type SyncerBigQuery struct {
	Conf           SyncerConf
	Client         BigQueryIface
	customIterator IteratorIface
}

type BigQueryIface interface {
	Query(string) *bigquery.Query
	Close() error
}

type IteratorIface interface {
	Next(dst interface{}) error
}

func NewSyncerBigQuery(conf SyncerConf) (*SyncerBigQuery, error) {
	projectID := conf.Service.Access["project_id"].(string)
	if projectID == "" {
		return nil, errors.New("project_id is required")
	}
	key := conf.Service.Access["access_key"].(string)
	if key == "" {
		return nil, errors.New("missing key")
	}
	optionWithKey := option.WithCredentialsJSON([]byte(key))

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID, optionWithKey)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	return &SyncerBigQuery{
		Client: client,
		Conf:   conf,
	}, nil
}

func (s *SyncerBigQuery) GetLastModified() (time.Time, error) {
	log.Fatal("not implemented")
	return time.Time{}, errors.New("not implemented")
}

func (s *SyncerBigQuery) GenerateSelectToSyncQuery() (string, error) {
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

func (s *SyncerBigQuery) ReadQuery(ctx context.Context, query *bigquery.Query) (IteratorIface, error) {
	if s.customIterator != nil {
		return s.customIterator, nil
	}
	return query.Read(ctx)
}

func (s *SyncerBigQuery) MakeQuery(ctx context.Context, query string) ([]map[string]any, error) {
	log.Println(query)
	cquery := s.Client.Query(query)
	rows, err := s.ReadQuery(ctx, cquery)
	if err != nil {
		return nil, err
	}

	results := []map[string]any{}
	var result map[string]bigquery.Value

	for err != iterator.Done {
		resultAny := map[string]any{}
		err = rows.Next(&result)
		if err != iterator.Done {
			for key, value := range result {
				resultAny[key] = any(value)
			}
			results = append(results, resultAny)
		}
	}
	if err != iterator.Done {
		return nil, err
	}

	return results, nil
}

func (s *SyncerBigQuery) SyncContactFields(db *sqlx.DB) (int, error) {
	var updated int
	query, err := s.GenerateSelectToSyncQuery()
	if err != nil {
		return 0, errors.Wrap(err, "error generating query")
	}

	results, err := s.MakeQuery(context.TODO(), query)
	if err != nil {
		return 0, errors.Wrap(err, "error executing query")
	}

	for _, r := range results {
		log.Println(r)
		for _, v := range s.Conf.Table.Columns {
			resultValue := r[v.Name]
			found := true
			// get contact field from flows
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			field, err := models.GetContactFieldByOrgAndLabel(ctx, db, s.Conf.SyncRules.OrgID, v.FieldMapName)
			if err != nil {
				slog.Error(fmt.Sprintf("field could not be found in flows. field: %s", v.Name), "err", err)
				found = false
			}
			if found {
				// if field exists in flows, update that field in contact
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				relationColumn, ok := r[s.Conf.Table.RelationColumn]
				if !ok {
					errMsg := fmt.Sprintf("row is not containing the configure relation_column configured as: %s", s.Conf.Table.RelationColumn)
					slog.Error(errMsg, "err", err)
				} else {
					err := models.UpdateContactField(ctx, db, relationColumn.(string), field.UUID, resultValue)
					if err != nil {
						errMsg := fmt.Sprintf("field could not be updated: %v", field)
						slog.Error(errMsg, "err", err)
					}
				}
			} else {
				// if field not exist, create it and create it in contact field column jsonb
				cf := models.NewContactField(
					v.FieldMapName,
					v.FieldMapName,
					models.ScanValueType(resultValue),
					s.Conf.SyncRules.OrgID,
					s.Conf.SyncRules.AdminID,
					s.Conf.SyncRules.AdminID)

				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				err := models.CreateContactField(ctx, db, cf)
				if err != nil {
					errMsg := fmt.Sprintf("error creating contact field: %v", v.FieldMapName)
					slog.Error(errMsg, "err", err)
				} else {
					err := models.UpdateContactField(ctx, db, r[s.Conf.Table.RelationColumn].(string), cf.UUID, resultValue)
					if err != nil {
						errMsg := fmt.Sprintf("error updating contact field: %v", v.FieldMapName)
						slog.Error(errMsg, "err", err)
					}
				}
			}
		}
		updated++
	}

	return updated, nil
}

func (s *SyncerBigQuery) Close() error {
	return s.Client.Close()
}

func (s *SyncerBigQuery) GetConfig() SyncerConf { return s.Conf }
