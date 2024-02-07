package syncer

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/athena/athenaiface"
	"github.com/pkg/errors"
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
	slog.Info(fmt.Sprintf("%s(%s) syncer making query: %s", s.GetConfig().ID, s.GetConfig().Service.Name, query))
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
				errMsg := *resultQueryExecution.QueryExecution.Status.AthenaError.ErrorMessage
				return nil, errors.Errorf("Query execution failed: %s", errMsg)
			}
		}
		time.Sleep(1 * time.Second)
	}

	resultRows := []map[string]interface{}{}
	currentPage := 0
	err = s.Client.GetQueryResultsPages(
		&athena.GetQueryResultsInput{QueryExecutionId: queryExecutionID},
		func(page *athena.GetQueryResultsOutput, lastPage bool) bool {
			{
				for i, row := range page.ResultSet.Rows {
					if currentPage == 0 && i == 0 {
						continue
					}

					currentRow := make(map[string]interface{})

					for j, data := range row.Data {
						columnName := *page.ResultSet.ResultSetMetadata.ColumnInfo[j].Name
						if data != nil && data.VarCharValue != nil {
							currentRow[columnName] = *data.VarCharValue
						}
					}
					resultRows = append(resultRows, currentRow)
				}
			}
			currentPage++
			return !lastPage
		})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get query results")
	}

	return resultRows, nil
}

func (s *SyncerAthena) Close() error {
	return nil
}

func (s *SyncerAthena) GetConfig() SyncerConf { return s.Conf }

func (s *SyncerAthena) GetTotalRows() (int, error) {
	var totalRows int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", s.Conf.Table.Name)

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
		return 0, errors.Wrap(err, "Error starting query execution")
	}

	queryExecutionID := startQueryOutput.QueryExecutionId
	for {
		resultQueryExecution, err := s.Client.GetQueryExecution(&athena.GetQueryExecutionInput{QueryExecutionId: queryExecutionID})
		if err != nil {
			return 0, errors.Wrap(err, "Error getting query execution")
		}

		if resultQueryExecution.QueryExecution != nil {
			if *resultQueryExecution.QueryExecution.Status.State == "SUCCEEDED" {
				break
			} else if *resultQueryExecution.QueryExecution.Status.State == "FAILED" {
				return 0, errors.Wrap(err, "Query execution failed")
			}
		}
		time.Sleep(1 * time.Second)
	}

	getQueryResultsOutput, err := s.Client.GetQueryResults(&athena.GetQueryResultsInput{
		QueryExecutionId: queryExecutionID,
	})
	if err != nil {
		return 0, errors.Wrap(err, "failed to get query results")
	}

	for i, row := range getQueryResultsOutput.ResultSet.Rows {
		if i == 0 {
			continue
		}
		for _, col := range row.Data {
			if col != nil && col.VarCharValue != nil {
				value, err := strconv.Atoi(*col.VarCharValue)
				if err != nil {
					return 0, errors.Wrap(err, "failed to get total rows count")
				}
				totalRows = value
			}
		}
	}

	return totalRows, nil
}
