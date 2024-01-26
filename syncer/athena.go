package syncer

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/athena/athenaiface"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.weni-ai/flows-field-syncer/configs"
)

type SyncerAthena struct {
	Conf                 SyncerConf
	Database             string
	ResultOutputLocation string
	WorkGroupName        string
	Client               athenaiface.AthenaAPI
}

const (
	CONF_AWS_ACCESS_KEY_ID     = "aws_access_key_id"
	CONF_AWS_SECRET_ACCESS_KEY = "aws_secret_access_key"
	CONF_AWS_REGION            = "aws_region"
	CONF_DATABASE              = "database"
	CONF_OUTPUT_LOCATION       = "output_location"
	CONF_WORKGROUP_NAME        = "workgroup_name"
)

func NewSyncerAthena(conf SyncerConf) (*SyncerAthena, error) {
	sess := session.Must(
		session.NewSession(
			aws.NewConfig().
				WithCredentials(
					credentials.NewStaticCredentials(
						conf.Service.Access[CONF_AWS_ACCESS_KEY_ID].(string),
						conf.Service.Access[CONF_AWS_SECRET_ACCESS_KEY].(string), "")).
				WithRegion(conf.Service.Access[CONF_AWS_REGION].(string)),
		))

	client := athena.New(sess)

	return &SyncerAthena{
		Conf:                 conf,
		Database:             conf.Service.Access[CONF_DATABASE].(string),
		ResultOutputLocation: conf.Service.Access[CONF_OUTPUT_LOCATION].(string),
		WorkGroupName:        conf.Service.Access[CONF_WORKGROUP_NAME].(string),
		Client:               client,
	}, nil
}

func (s *SyncerAthena) GetLastModified() (time.Time, error) {
	return time.Time{}, errors.New("not implemented yet")
}

func (s *SyncerAthena) GenerateSelectToSyncQuery(offset, limit int) (string, error) {
	var columns []string
	table := s.Conf.Table

	columns = append(columns, table.RelationColumn)

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

func (s *SyncerAthena) MakeQuery(ctx context.Context, query string) ([]map[string]any, error) {
	slog.Info(fmt.Sprintf("making query: %s, for %s with org ID %d", query, s.GetConfig().Service.Name, s.GetConfig().SyncRules.OrgID))
	queryInput := &athena.StartQueryExecutionInput{
		QueryString: aws.String(query),
		QueryExecutionContext: &athena.QueryExecutionContext{
			Database: aws.String(s.Database),
		},
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: aws.String(s.ResultOutputLocation),
		},
		WorkGroup: aws.String(s.WorkGroupName),
	}

	startQueryOutput, err := s.Client.StartQueryExecution(queryInput)
	if err != nil {
		return nil, errors.Wrap(err, "Error starting query execution")
	}

	queryExecutionID := startQueryOutput.QueryExecutionId
	for {
		resultQueryExecution, err := s.Client.GetQueryExecution(&athena.GetQueryExecutionInput{QueryExecutionId: queryExecutionID})
		if err != nil {
			return nil, errors.Wrap(err, "Error getting query execution")
		}

		if resultQueryExecution.QueryExecution != nil {
			if *resultQueryExecution.QueryExecution.Status.State == "SUCCEEDED" {
				break
			} else if *resultQueryExecution.QueryExecution.Status.State == "FAILED" {
				return nil, errors.Wrap(err, "Query execution failed")
			}
		}
		time.Sleep(1 * time.Second)
	}

	getQueryResultsOutput, err := s.Client.GetQueryResults(&athena.GetQueryResultsInput{
		QueryExecutionId: queryExecutionID,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get query results")
	}

	resultRows := []map[string]interface{}{}

	for i, row := range getQueryResultsOutput.ResultSet.Rows {
		if i == 0 {
			continue
		}

		currentRow := make(map[string]interface{})

		for j, data := range row.Data {
			columnName := *getQueryResultsOutput.ResultSet.ResultSetMetadata.ColumnInfo[j].Name
			if data != nil && data.VarCharValue != nil {
				currentRow[columnName] = *data.VarCharValue
			}
		}
		resultRows = append(resultRows, currentRow)
	}

	return resultRows, nil
}

func (s *SyncerAthena) SyncContactFields(db *sqlx.DB) (int, error) {
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

func (s *SyncerAthena) Close() error {
	return nil
}

func (s *SyncerAthena) GetConfig() SyncerConf { return s.Conf }
