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
	"github.weni-ai/flows-field-syncer/configs"
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

func (s *SyncerPG) GenerateSelectToSyncQuery(offset, limit int) (string, error) {
	var columns []string
	table := s.Conf.Table

	columns = append(columns, table.RelationColumn) // include column fk

	for _, column := range table.Columns {
		columns = append(columns, column.Name)
	}
	columnList := strings.Join(columns, ", ")

	query := fmt.Sprintf("SELECT %s FROM %s OFFSET %d", columnList, table.Name, offset)
	if limit != 0 {
		query = fmt.Sprintf("%s LIMIT %d", query, limit)
	}
	return query, nil
}

func (s *SyncerPG) MakeQuery(ctx context.Context, query string) ([]map[string]any, error) {
	slog.Info(fmt.Sprintf("making query: %s", query))
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
	rows.Close()
	return results, nil
}

func (s *SyncerPG) SyncContactFields(db *sqlx.DB) (int, error) {
	var updated int
	conf := configs.GetConfig()
	batchSize := conf.BatchSize
	for offset := 0; ; offset += batchSize {
		query, err := s.GenerateSelectToSyncQuery(offset, batchSize)
		if err != nil {
			return 0, errors.Wrap(err, "error generating query")
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*600)
		defer cancel()
		results, err := s.MakeQuery(ctx, query)
		if err != nil {
			return 0, errors.Wrap(err, "error executing query")
		}

		// if no results stop perform sync
		if len(results) == 0 {
			break
		}

		updatedPerformed, err := performSync(s.GetConfig(), db, results)
		updated += updatedPerformed
		if err != nil {
			return updated, nil
		}

		if batchSize == 0 {
			break
		}
	}

	return updated, nil
}

func (s *SyncerPG) Close() error {
	return s.DB.Close()
}

func (s *SyncerPG) GetConfig() SyncerConf { return s.Conf }
