package syncer

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type SyncerConfRepository interface {
	Create(syncerConf SyncerConf) error
	GetByID(id string) (SyncerConf, error)
	GetAll() ([]SyncerConf, error)
	Update(id string, syncerConf SyncerConf) error
	Delete(id string) error
}

type syncerConfRepository struct {
	collection *mongo.Collection
}

func NewSyncerConfRepository(db *mongo.Database) SyncerConfRepository {
	collection := db.Collection("syncerconf")
	return &syncerConfRepository{collection}
}

func (r *syncerConfRepository) Create(syncerConf SyncerConf) error {
	_, err := r.collection.InsertOne(context.Background(), syncerConf)
	return err
}

func (r *syncerConfRepository) GetByID(id string) (SyncerConf, error) {
	var syncerConf SyncerConf
	err := r.collection.FindOne(context.Background(), bson.M{"_id": id}).Decode(&syncerConf)
	return syncerConf, err
}

func (r *syncerConfRepository) GetAll() ([]SyncerConf, error) {
	syncerConfs := []SyncerConf{}
	c, err := r.collection.Find(context.Background(), bson.M{})
	if err != nil {
		return nil, err
	}
	defer c.Close(context.Background())

	for c.Next(context.Background()) {
		var syncerConf SyncerConf
		if err := c.Decode(&syncerConf); err != nil {
			return nil, err
		}
		syncerConfs = append(syncerConfs, syncerConf)
	}

	if err := c.Err(); err != nil {
		return nil, err
	}

	return syncerConfs, err
}

func (r *syncerConfRepository) Update(id string, syncerConf SyncerConf) error {
	_, err := r.collection.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$set": syncerConf})
	return err
}

func (r *syncerConfRepository) Delete(id string) error {
	_, err := r.collection.DeleteOne(context.Background(), bson.M{"_id": id})
	return err
}

type SyncerLogRepository interface {
	Create(syncerLog SyncerLog) error
	GetByID(id string) (*SyncerLog, error)
	Update(id string, syncerLog SyncerLog) error
	Delete(id string) error
	DeleteOlderThan(createdAt time.Time) (int64, error)
}

type syncerLogRepository struct {
	collection *mongo.Collection
}

func NewSyncerLogRepository(db *mongo.Database) SyncerLogRepository {
	collection := db.Collection("syncerlog")
	return &syncerLogRepository{collection}
}

func (r *syncerLogRepository) Create(syncerLog SyncerLog) error {
	_, err := r.collection.InsertOne(context.Background(), syncerLog)
	return err
}

func (r *syncerLogRepository) GetByID(id string) (*SyncerLog, error) {
	var syncerLog *SyncerLog
	err := r.collection.FindOne(context.Background(), bson.M{"_id": id}).Decode(&syncerLog)
	return syncerLog, err
}

func (r *syncerLogRepository) Update(id string, syncerLog SyncerLog) error {
	_, err := r.collection.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$set": syncerLog})
	return err
}

func (r *syncerLogRepository) Delete(id string) error {
	_, err := r.collection.DeleteOne(context.Background(), bson.M{"_id": id})
	return err
}

func (r *syncerLogRepository) DeleteOlderThan(createdAt time.Time) (int64, error) {
	result, err := r.collection.DeleteMany(context.Background(), bson.M{"creted_at": bson.M{"$lt": createdAt}})
	if err != nil {
		return 0, err
	}
	return result.DeletedCount, nil
}
