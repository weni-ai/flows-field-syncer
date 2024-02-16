package syncer

import (
	"context"
	"fmt"
	"log"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/pkg/errors"
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

func (s *SyncerBigQuery) BaseQuery() string {
	var columns []string
	table := s.Conf.Table

	columns = append(columns, table.RelationColumn) // include column fk

	for _, column := range table.Columns {
		columns = append(columns, column.Name)
	}
	columnList := strings.Join(columns, ", ")
	bq := fmt.Sprintf("SELECT %s FROM %s ", columnList, table.Name)
	return bq
}

func (s *SyncerBigQuery) GenerateSelectToSyncQuery(offset, limit int, conditionList []string) (string, error) {
	baseQuery := s.BaseQuery()
	var query string
	switch s.Conf.SyncRules.Strategy {
	case StrategyTypeContactURN:
		for i, str := range conditionList {
			conditionList[i] = "'" + str + "'"
		}
		query = fmt.Sprintf("%s WHERE %s in (%s)", baseQuery, s.Conf.Table.RelationColumn, strings.Join(conditionList, ", "))
	case StrategyTypePull:
	default:
		query = fmt.Sprintf("%s OFFSET %d", baseQuery, offset)
	}
	if limit != 0 {
		query = fmt.Sprintf("%s LIMIT %d", query, limit)
	}
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

func (s *SyncerBigQuery) Close() error {
	return s.Client.Close()
}

func (s *SyncerBigQuery) GetConfig() SyncerConf { return s.Conf }

func (s *SyncerBigQuery) GetTotalRows() (int, error) {
	return 0, nil
}
