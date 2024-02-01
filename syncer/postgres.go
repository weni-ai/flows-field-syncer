package syncer

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.weni-ai/flows-field-syncer/configs"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
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

func (s *SyncerPG) SyncContactFields(db *sqlx.DB) (int, error) {
	var startTime = time.Now()
	var updated int
	var updatedMutex sync.Mutex
	conf := configs.GetConfig()
	batchSize := conf.BatchSize
	maxGoroutines := conf.MaxGoroutines
	totalRows, err := s.getTotalRows(db)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get total rows")
	}

	sem := semaphore.NewWeighted(int64(maxGoroutines))
	var eg errgroup.Group
	var count int
	var countMutex sync.Mutex

	for offset := 0; offset < totalRows; offset += batchSize {

		currOffset := offset

		if err := sem.Acquire(context.TODO(), 1); err != nil {
			return 0, err
		}

		eg.Go(func() error {
			defer sem.Release(1)
			query, err := s.GenerateSelectToSyncQuery(currOffset, batchSize)
			if err != nil {
				return errors.Wrap(err, "error generating query")
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*600)
			defer cancel()
			results, err := s.MakeQuery(ctx, query)
			if err != nil {
				return errors.Wrap(err, "error executing query")
			}

			updatedPerformed, err := performSync(s.GetConfig(), db, results)
			updatedMutex.Lock()
			updated += updatedPerformed
			updatedMutex.Unlock()
			if err != nil {
				return errors.Wrap(err, "error executing sync")
			}

			countMutex.Lock()
			count += len(results)
			countMutex.Unlock()

			// Calculate percentage completion and elapsed time
			percentage := float64(count) / float64(totalRows) * 100
			elapsedTime := time.Since(startTime)
			estimatedTimeRemaining := (elapsedTime / time.Duration(count)) * time.Duration(totalRows-count)

			slog.Info(fmt.Sprintf("%s(%s) Progress: %.2f%%, Elapsed time: %s, Estimated remaining time: %s\n", s.Conf.ID, s.Conf.Service.Name, percentage, elapsedTime, estimatedTimeRemaining))
			return nil
		})

		if batchSize == 0 {
			break
		}
	}

	if err := eg.Wait(); err != nil {
		log.Fatal(err)
	}

	return updated, nil
}

func (s *SyncerPG) Close() error {
	return s.DB.Close()
}

func (s *SyncerPG) GetConfig() SyncerConf { return s.Conf }

func (s *SyncerPG) getTotalRows(db *sqlx.DB) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	var totalRows int
	err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", s.Conf.Table.Name)).Scan(&totalRows)
	if err != nil {
		return 0, err
	}
	return totalRows, nil
}
