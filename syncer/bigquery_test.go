package syncer

import (
	"fmt"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.weni-ai/flows-field-syncer/configs"
	"google.golang.org/api/iterator"
)

func TestSyncerBigquery(t *testing.T) {
	conf, err := GetConf("./testdata/bigquery.json")
	if err != nil {
		t.Fatal(err)
	}

	mockClient := new(MockBigQueryClient)
	syncerBigquery := &SyncerBigQuery{
		Client: mockClient,
		Conf:   *conf,
	}

	expectedResults := &MockRowIterator{
		Results: []map[string]bigquery.Value{
			{
				"external_id": "123",
				"region":      "feitosa",
				"state":       "alagoas",
			},
		},
	}

	syncerBigquery.customIterator = expectedResults

	config := configs.NewConfig()
	db, err := sqlx.Open("postgres", config.FlowsDB)
	if err != nil {
		t.Fatal(err)
	}

	total, err := syncerBigquery.SyncContactFields(db)
	assert.Nil(t, err)
	assert.Equal(t, 1, total)
}

type MockBigQueryClient struct {
	mock.Mock
}

func (m *MockBigQueryClient) Query(query string) *bigquery.Query {
	return nil
}

func (m *MockBigQueryClient) Close() error { return nil }

type MockRowIterator struct {
	Results []map[string]bigquery.Value
	Index   int
}

func (m *MockRowIterator) Next(dest interface{}) error {
	if m.Index >= len(m.Results) {
		return iterator.Done
	}

	result := m.Results[m.Index]
	m.Index++

	switch dest := dest.(type) {
	case *map[string]bigquery.Value:
		*dest = result
	default:
		return fmt.Errorf("target type not supported")
	}

	return nil
}
