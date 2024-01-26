package models

import (
	"context"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.weni-ai/flows-field-syncer/configs"
)

func TestContact(t *testing.T) {
	config := configs.GetConfig()
	db, err := sqlx.Open("postgres", config.FlowsDB)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	err = UpdateContactFieldByURN(ctx, db, "938623661", 1, "b8f598c1-5e3f-4675-90b9-0891c72088ee", "nowhere")
	assert.NoError(t, err)
}
