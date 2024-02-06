package syncer

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

type SyncerPG struct {
	Conf SyncerConf
	DB   *sqlx.DB
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
	slog.Info(fmt.Sprintf("%s(%s) syncer making query: %s", s.GetConfig().ID, s.GetConfig().Service.Name, query))
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

func (s *SyncerPG) Close() error {
	return s.DB.Close()
}

func (s *SyncerPG) GetConfig() SyncerConf { return s.Conf }

func (s *SyncerPG) GetTotalRows() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	var totalRows int
	err := s.DB.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", s.Conf.Table.Name)).Scan(&totalRows)
	if err != nil {
		return 0, err
	}
	return totalRows, nil
}
