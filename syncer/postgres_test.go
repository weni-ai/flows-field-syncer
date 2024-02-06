package syncer

import (
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.weni-ai/flows-field-syncer/configs"
)

func TestSyncerPostgres(t *testing.T) {
	conf, err := GetConf("./testdata/postgres_relation_contact.json")
	if err != nil {
		t.Fatal(err)
	}

	syncerPostgres, err := NewSyncer(*conf)
	assert.NoError(t, err)

	config := configs.GetConfig()
	db, err := sqlx.Open("postgres", config.FlowsDB)
	if err != nil {
		t.Fatal(err)
	}

	total, err := SyncContactFields(db, syncerPostgres)
	assert.Nil(t, err)
	assert.Equal(t, 1, total)
}
