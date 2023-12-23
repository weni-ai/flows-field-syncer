package syncer

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/athena/athenaiface"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.weni-ai/flows-field-syncer/configs"
)

func TestSyncerAthena(t *testing.T) {
	conf, err := GetConf("./testdata/athena.json")
	if err != nil {
		t.Fatal(err)
	}

	athenaMockClient := &mockAthenaClient{
		startQueryExecutionOutput: &athena.StartQueryExecutionOutput{
			QueryExecutionId: aws.String("query_execution_id_mock"),
		},
		getQueryExecutionOutput: &athena.GetQueryExecutionOutput{
			QueryExecution: &athena.QueryExecution{
				Status: &athena.QueryExecutionStatus{
					State: aws.String("SUCCEEDED"),
				},
			},
		},
		getQueryResultsOutput: &athena.GetQueryResultsOutput{
			ResultSet: &athena.ResultSet{
				ResultSetMetadata: &athena.ResultSetMetadata{
					ColumnInfo: []*athena.ColumnInfo{
						{Name: sp("id")},
						{Name: sp("external_id")},
						{Name: sp("region")},
						{Name: sp("state")},
						{Name: sp("city")},
						{Name: sp("mcc_code")},
						{Name: sp("accreditation_status")},
						{Name: sp("creeated_on")},
						{Name: sp("modified_on")},
					},
				},
				Rows: []*athena.Row{
					{
						Data: []*athena.Datum{
							{VarCharValue: aws.String("id")},
							{VarCharValue: aws.String("external_id")},
							{VarCharValue: aws.String("region")},
							{VarCharValue: aws.String("state")},
							{VarCharValue: aws.String("city")},
							{VarCharValue: aws.String("mcc_code")},
							{VarCharValue: aws.String("accreditation_status")},
							{VarCharValue: aws.String("creeated_on")},
							{VarCharValue: aws.String("modified_on")},
						},
					},
					{
						Data: []*athena.Datum{
							{VarCharValue: aws.String("1")},
							{VarCharValue: aws.String("d5499c47-9b4f-485d-acc8-a7555864a493")},
							{VarCharValue: aws.String("feitosa")},
							{VarCharValue: aws.String("Alagoas")},
							{VarCharValue: aws.String("Maceió")},
							{VarCharValue: aws.String("code01")},
							{VarCharValue: aws.String("active")},
							{VarCharValue: aws.String("2023-11-22 17:54:51.877")},
							{VarCharValue: aws.String("2023-11-22 17:54:51.877")},
						},
					},
				},
			},
		},
		startQueryExecutionOutputError: nil,
		getQueryExecutionOutputError:   nil,
		getQueryResultsOutputError:     nil,
	}

	syncerAthena := &SyncerAthena{
		Conf:                 *conf,
		Database:             conf.Service.Access[CONF_DATABASE].(string),
		ResultOutputLocation: conf.Service.Access[CONF_OUTPUT_LOCATION].(string),
		Client:               athenaMockClient,
	}

	config := configs.NewConfig()
	db, err := sqlx.Open("postgres", config.FlowsDB)
	if err != nil {
		t.Fatal(err)
	}

	total, err := syncerAthena.SyncContactFields(db)
	assert.Nil(t, err)
	assert.Equal(t, 1, total)
}

type mockAthenaClient struct {
	athenaiface.AthenaAPI
	startQueryExecutionOutput      *athena.StartQueryExecutionOutput
	startQueryExecutionOutputError error
	getQueryExecutionOutput        *athena.GetQueryExecutionOutput
	getQueryExecutionOutputError   error
	getQueryResultsOutput          *athena.GetQueryResultsOutput
	getQueryResultsOutputError     error
}

func (m *mockAthenaClient) StartQueryExecution(input *athena.StartQueryExecutionInput) (*athena.StartQueryExecutionOutput, error) {
	return m.startQueryExecutionOutput, m.startQueryExecutionOutputError
}

func (m *mockAthenaClient) GetQueryExecution(input *athena.GetQueryExecutionInput) (*athena.GetQueryExecutionOutput, error) {
	return m.getQueryExecutionOutput, m.getQueryExecutionOutputError
}

func (m *mockAthenaClient) GetQueryResults(input *athena.GetQueryResultsInput) (*athena.GetQueryResultsOutput, error) {
	return m.getQueryResultsOutput, m.getQueryResultsOutputError
}

func GetConf(confPath string) (*SyncerConf, error) {
	confsFile := confPath
	file, err := os.Open(confsFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	conf := &SyncerConf{}

	err = json.NewDecoder(file).Decode(&conf)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

func sp(s string) *string { return &s }
