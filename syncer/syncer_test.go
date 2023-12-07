package syncer_test

import (
	"encoding/json"
	"log"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.weni-ai/flows-field-syncer/syncer"
)

const dbdsn = "postgres://temba:temba@localhost/temba?sslmode=disable"

func TestAthenaSync(t *testing.T) {
	db, err := sqlx.Open("postgres", dbdsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	confsFile := "./testdata/athena.json"
	file, err := os.Open(confsFile)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var confs []syncer.SyncerConf

	err = json.NewDecoder(file).Decode(&confs)
	if err != nil {
		t.Fatal(err)
	}

	if len(confs) >= 1 {
		scr, err := syncer.NewSyncer(confs[0])
		if err != nil {
			t.Fatal(err)
		}
		_, err = scr.SyncContactFields(db)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestBigquerySync(t *testing.T) {
	db, err := sqlx.Open("postgres", dbdsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	confsFile := "./testdata/bigquery.json"
	file, err := os.Open(confsFile)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var conf syncer.SyncerConf

	err = json.NewDecoder(file).Decode(&conf)
	if err != nil {
		t.Fatal(err)
	}

	scr, err := syncer.NewSyncer(conf)
	if err != nil {
		t.Fatal(err)
	}
	scr.SyncContactFields(db)
}

func TestSync(t *testing.T) {
	db, err := sqlx.Open("postgres", dbdsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	confsFile := "./testdata/postgre.json"
	file, err := os.Open(confsFile)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var confs []syncer.SyncerConf

	err = json.NewDecoder(file).Decode(&confs)
	if err != nil {
		t.Fatal(err)
	}

	if len(confs) >= 1 {
		scr, err := syncer.NewSyncer(confs[0])
		if err != nil {
			t.Fatal(err)
		}
		scr.SyncContactFields(db)
	}
}
