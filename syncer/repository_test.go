package syncer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestSyncerConfRepository(t *testing.T) {
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(context.Background())

	db := client.Database("testdb")
	db.Collection("syncerconf").Drop(context.TODO())

	repo := NewSyncerConfRepository(db)

	t.Run("Create", func(t *testing.T) {
		syncerConf := SyncerConf{ID: "1", Service: SyncerService{Name: "Test"}}
		_, err := repo.Create(syncerConf)
		assert.NoError(t, err)
	})

	t.Run("GetByID", func(t *testing.T) {
		expected := SyncerConf{ID: "1", Service: SyncerService{Name: "Test"}}
		result, err := repo.GetByID("1")
		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("GetAll", func(t *testing.T) {
		expected := []SyncerConf{{ID: "1", Service: SyncerService{Name: "Test"}}}
		result, err := repo.GetAll()
		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("Update", func(t *testing.T) {
		newSyncerConf := SyncerConf{ID: "1", Service: SyncerService{Name: "Updated"}}
		err := repo.Update("1", newSyncerConf)
		assert.NoError(t, err)

		result, err := repo.GetByID("1")
		assert.NoError(t, err)
		assert.Equal(t, newSyncerConf, result)
	})

	t.Run("Delete", func(t *testing.T) {
		err := repo.Delete("1")
		assert.NoError(t, err)

		result, err := repo.GetByID("1")
		assert.Error(t, err)
		assert.Equal(t, SyncerConf{}, result)
	})
}

func TestSyncerLogRepository(t *testing.T) {
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(context.Background())

	db := client.Database("testdb")
	db.Collection("syncerlog").Drop(context.TODO())

	repo := NewSyncerLogRepository(db)

	t.Run("Create", func(t *testing.T) {
		syncerLog := SyncerLog{ID: "1", OrgID: 123, ConfID: "conf1", Details: "Test", LogType: "info"}
		err := repo.Create(syncerLog)
		assert.NoError(t, err)
	})

	t.Run("GetByID", func(t *testing.T) {
		expected := &SyncerLog{ID: "1", OrgID: 123, ConfID: "conf1", Details: "Test", LogType: "info"}
		result, err := repo.GetByID("1")
		assert.NoError(t, err)
		assert.Equal(t, expected.Details, result.Details)
	})

	t.Run("Update", func(t *testing.T) {
		newSyncerLog := &SyncerLog{ID: "1", OrgID: 123, ConfID: "conf1", Details: "Updated", LogType: "info"}
		err := repo.Update("1", *newSyncerLog)
		assert.NoError(t, err)

		result, err := repo.GetByID("1")
		assert.NoError(t, err)
		assert.Equal(t, newSyncerLog.Details, result.Details)
	})

	t.Run("Delete", func(t *testing.T) {
		err := repo.Delete("1")
		assert.NoError(t, err)

		result, err := repo.GetByID("1")
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("DeleteOlderThan", func(t *testing.T) {
		currentTime := time.Now()
		oldTime := currentTime.Add(-time.Hour * 24)

		oldLogs := []SyncerLog{
			{ID: "1", OrgID: 123, ConfID: "conf1", Details: "Old log 1", CretedAt: oldTime, UpdatedAt: oldTime, LogType: "info"},
			{ID: "2", OrgID: 123, ConfID: "conf1", Details: "Old log 2", CretedAt: oldTime, UpdatedAt: oldTime, LogType: "error"},
		}

		for _, log := range oldLogs {
			err := repo.Create(log)
			assert.NoError(t, err)
		}

		recentLog := SyncerLog{ID: "3", OrgID: 123, ConfID: "conf1", Details: "Recent log", CretedAt: currentTime, UpdatedAt: currentTime, LogType: "info"}
		err = repo.Create(recentLog)
		assert.NoError(t, err)

		deletedCount, err := repo.DeleteOlderThan(currentTime)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(oldLogs)), deletedCount)

		for _, log := range oldLogs {
			result, err := repo.GetByID(log.ID)
			assert.Error(t, err)
			assert.Nil(t, result)
		}

		result, err := repo.GetByID(recentLog.ID)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
}
