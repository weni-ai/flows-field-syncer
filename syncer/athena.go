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
	"github.weni-ai/flows-field-syncer/models"
)

type SyncerAthena struct {
	Conf                 SyncerConf
	Database             string
	ResultOutputLocation string
	Client               athenaiface.AthenaAPI
}

const (
	CONF_AWS_ACCESS_KEY_ID     = "aws_access_key_id"
	CONF_AWS_SECRET_ACCESS_KEY = "aws_secret_access_key"
	CONF_AWS_REGION            = "aws_region"
	CONF_DATABASE              = "database"
	CONF_OUTPUT_LOCATION       = "output_location"
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
		Client:               client,
	}, nil
}

func (s *SyncerAthena) GetLastModified() (time.Time, error) {
	return time.Time{}, errors.New("not implemented yet")
}

func (s *SyncerAthena) GenerateSelectToSyncQuery() (string, error) {
	var columns []string
	table := s.Conf.Table

	columns = append(columns, table.RelationColumn)

	for _, column := range table.Columns {
		columns = append(columns, column.Name)
	}
	columnList := strings.Join(columns, ", ")

	query := fmt.Sprintf("SELECT %s FROM %s", columnList, table.Name)
	return query, nil
}

func (s *SyncerAthena) MakeQuery(ctx context.Context, query string) ([]map[string]any, error) {
	queryInput := &athena.StartQueryExecutionInput{
		QueryString: aws.String(query),
		QueryExecutionContext: &athena.QueryExecutionContext{
			Database: aws.String(s.Database),
		},
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: aws.String(s.ResultOutputLocation),
		},
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

			currentRow[columnName] = *data.VarCharValue
		}

		resultRows = append(resultRows, currentRow)
	}

	return resultRows, nil
}

func (s *SyncerAthena) SyncContactFields(db *sqlx.DB) (int, error) {
	var updated int
	query, err := s.GenerateSelectToSyncQuery()
	if err != nil {
		return 0, err
	}

	results, err := s.MakeQuery(context.TODO(), query)
	if err != nil {
		return 0, err
	}

	for _, r := range results {
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
				err := models.UpdateContactField(ctx, db, r[s.Conf.Table.RelationColumn].(string), field.UUID, resultValue)
				if err != nil {
					errMsg := fmt.Sprintf("field could not be updated: %v", field)
					slog.Error(errMsg, "err", err)
				}
			} else {
				// if field not exist, create it and create it in contact field column jsonb
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				cf := models.NewContactField(
					v.FieldMapName,
					v.FieldMapName,
					models.ScanValueType(resultValue),
					s.Conf.SyncRules.OrgID,
					s.Conf.SyncRules.AdminID,
					s.Conf.SyncRules.AdminID)

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

func (s *SyncerAthena) Close() error {
	return nil
}

func (s *SyncerAthena) GetConfig() SyncerConf { return s.Conf }
