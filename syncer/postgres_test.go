package syncer

import (
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.weni-ai/flows-field-syncer/configs"
)

func TestSyncerPostgres(t *testing.T) {
	conf, err := GetConf("./testdata/postgre.json")
	if err != nil {
		t.Fatal(err)
	}

	syncerPostgres, err := NewSyncer(*conf)
	assert.NoError(t, err)

	config := configs.NewConfig()
	db, err := sqlx.Open("postgres", config.FlowsDB)
	if err != nil {
		t.Fatal(err)
	}

	total, err := syncerPostgres.SyncContactFields(db)
	assert.Nil(t, err)
	assert.Equal(t, 1, total)
}
