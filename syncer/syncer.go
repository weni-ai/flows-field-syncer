package syncer

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"github.weni-ai/flows-field-syncer/configs"
	"github.weni-ai/flows-field-syncer/models"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const (
	TypePostgres = "postgres"
	TypeBigQuery = "bigquery"
	TypeAthena   = "athena"
)

const (
	RelationTypeURN     = "urn"
	RelationTypeContact = "contact"
)

const (
	LogTypeInfo  = "info"
	LogTypeError = "error"
)

const (
	StrategyTypePull       = "pull"
	StrategyTypeContactURN = "contact_urn"
)

type Syncer interface {
	GenerateSelectToSyncQuery(int, int, []string) (string, error)
	MakeQuery(context.Context, string) ([]map[string]any, error)
	Close() error
	GetConfig() SyncerConf
	GetTotalRows() (int, error)
}

type SyncerBase struct {
}

func NewSyncer(conf SyncerConf) (Syncer, error) {
	switch conf.Service.Type {
	case TypePostgres:
		return NewSyncerPG(conf)
	case TypeBigQuery:
		return NewSyncerBigQuery(conf)
	case TypeAthena:
		return NewSyncerAthena(conf)
	}
	return nil, errors.New("service type not supported")
}

type SyncerConf struct {
	ID        string        `bson:"_id" json:"id,omitempty"`
	Service   SyncerService `bson:"service" json:"service"`
	SyncRules struct {
		ScheduleTime string `bson:"schedule_time" json:"schedule_time"`
		Interval     int    `bson:"interval" json:"interval"`
		OrgID        int64  `bson:"org_id" json:"org_id"`
		AdminID      int64  `bson:"admin_id" json:"admin_id"`
		Strategy     string `bson:"strategy" json:"strategy"`
		Schema       string `bson:"schema" json:"schema"`
	} `bson:"sync_rules" json:"sync_rules"`
	Table    SyncerTable `bson:"table" json:"table"`
	IsActive bool        `bson:"is_active" json:"is_active"`
}

type SyncerService struct {
	Name   string                 `bson:"name" json:"name"`
	Type   string                 `bson:"type" json:"type"`
	Access map[string]interface{} `bson:"access" json:"access"`
}

type SyncerTable struct {
	Name             string         `bson:"name" json:"name"`
	TableDestination string         `bson:"table_destination" json:"table_destination"`
	RelationType     string         `bson:"relation_type" json:"relation_type"`
	RelationColumn   string         `bson:"relation_column" json:"relation_column"`
	Columns          []SyncerColumn `bson:"columns" json:"columns"`
}

type SyncerColumn struct {
	Name         string        `bson:"name" json:"name"`
	FieldMapName string        `bson:"field_map_name" json:"field_map_name"`
	NestedFields []NestedField `bson:"nested_fields" json:"nested_fields"`
	NestedType   string        `bson:"nested_type" json:"nested_type"`
}

type NestedField struct {
	Attribute    string `bson:"attribute" json:"attribute"`
	FieldMapName string `bson:"field_map_name" json:"field_map_name"`
}

type SyncerLog struct {
	ID        string      `bson:"_id" json:"id"`
	OrgID     int64       `bson:"org_id" json:"org_id"`
	ConfID    string      `bson:"conf_id" json:"conf_id"`
	Details   interface{} `bson:"details" json:"details"`
	LogType   string      `bson:"type" json:"log_type"`
	CretedAt  time.Time   `bson:"creted_at" json:"creted_at"`
	UpdatedAt time.Time   `bson:"updated_at" json:"updated_at"`
}

func NewSyncerLog(orgID int64, confID string, details interface{}, logType string) *SyncerLog {
	return &SyncerLog{
		OrgID:     orgID,
		ConfID:    confID,
		Details:   details,
		LogType:   logType,
		CretedAt:  time.Now(),
		UpdatedAt: time.Now(),
	}
}

func (c *SyncerConf) Validate() error {
	return errors.New("not implemented")
}

type SyncerLogCleaner interface {
}

func applySync(conf SyncerConf, db *sqlx.DB, results []map[string]any) (int, error) {
	slog.Info(fmt.Sprintf("%s(%s) syncer apply sync on destination", conf.ID, conf.Service.Name))
	var updated int
	// for each contact
	for _, r := range results {
		// update fields
		for _, v := range conf.Table.Columns {
			resultValue := r[v.Name]
			contact := r[conf.Table.RelationColumn].(string)
			// if nestedfields
			if len(v.NestedFields) > 0 {
				nestedFields, ok := resultValue.(string)
				if ok {
					var fieldsMap map[string]string
					switch v.NestedType {
					case "json":
					case "struct":
						fieldsMap = StringStructToMap(nestedFields)
					}
					for _, nestedField := range v.NestedFields {
						fieldValue := fieldsMap[nestedField.Attribute]
						fieldKey := nestedField.FieldMapName

						// get contact field from flows
						found := true
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer cancel()
						field, err := models.GetContactFieldByOrgAndKey(ctx, db, conf.SyncRules.OrgID, fieldKey)
						if err != nil {
							slog.Error(fmt.Sprintf("field could not be found in flows. field: %s", v.Name), "err", err)
							found = false
						}
						if found {
							// if field exists in flows, update that field in contact
							ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
							defer cancel()
							switch conf.Table.RelationType {
							case RelationTypeContact:
								err := models.UpdateContactField(ctx, db, contact, field.UUID, fieldValue)
								if err != nil {
									errMsg := fmt.Sprintf("field could not be updated: %v", field)
									slog.Error(errMsg, "err", err)
									return updated, errors.Wrap(err, errMsg)
								}
							case RelationTypeURN:
								err := models.UpdateContactFieldByURN(ctx, db, contact, conf.SyncRules.OrgID, field.UUID, fieldValue)
								if err != nil {
									errMsg := fmt.Sprintf("field could not be updated: %v", field)
									slog.Error(errMsg, "err", err)
									return updated, errors.Wrap(err, errMsg)
								}
							}
						} else {
							// if field not exist, create it and create it in contact field column jsonb
							ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
							defer cancel()
							cf := models.NewContactField(
								nestedField.FieldMapName,
								nestedField.FieldMapName,
								models.ScanValueType(fieldValue),
								conf.SyncRules.OrgID,
								conf.SyncRules.AdminID,
								conf.SyncRules.AdminID)

							err := models.CreateContactField(ctx, db, cf)
							if err != nil {
								errMsg := fmt.Sprintf("error creating contact field: %v", nestedField.FieldMapName)
								slog.Error(errMsg, "err", err)
								return updated, errors.Wrap(err, errMsg)
							} else {
								switch conf.Table.RelationType {
								case RelationTypeContact:
									err := models.UpdateContactField(ctx, db, contact, cf.UUID, fieldValue)
									if err != nil {
										errMsg := fmt.Sprintf("error updating contact field: %v", nestedField.FieldMapName)
										slog.Error(errMsg, "err", err)
										return updated, errors.Wrap(err, errMsg)
									}
								case RelationTypeURN:
									err := models.UpdateContactFieldByURN(ctx, db, contact, conf.SyncRules.OrgID, cf.UUID, fieldValue)
									if err != nil {
										errMsg := fmt.Sprintf("error updating contact field: %v", nestedField.FieldMapName)
										slog.Error(errMsg, "err", err)
										return updated, errors.Wrap(err, errMsg)
									}
								}
							}
						}

					}
				}

				// if mapping is not for jsonb or struct field
			} else {
				// get contact field from flows
				found := true
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				field, err := models.GetContactFieldByOrgAndKey(ctx, db, conf.SyncRules.OrgID, v.FieldMapName)
				if err != nil {
					slog.Error(fmt.Sprintf("field could not be found in flows. field: %s", v.Name), "err", err)
					found = false
				}

				if found {
					// if field exists in flows, update that field in contact
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					switch conf.Table.RelationType {
					case RelationTypeContact:
						err := models.UpdateContactField(ctx, db, contact, field.UUID, resultValue)
						if err != nil {
							errMsg := fmt.Sprintf("field could not be updated: %v", field)
							slog.Error(errMsg, "err", err)
							return updated, errors.Wrap(err, errMsg)
						}
					case RelationTypeURN:
						err := models.UpdateContactFieldByURN(ctx, db, contact, conf.SyncRules.OrgID, field.UUID, resultValue)
						if err != nil {
							errMsg := fmt.Sprintf("field could not be updated: %v", field)
							slog.Error(errMsg, "err", err)
							return updated, errors.Wrap(err, errMsg)
						}
					}
				} else {
					// if field not exist, create it and create it in contact field column jsonb
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()
					cf := models.NewContactField(
						v.FieldMapName,
						v.FieldMapName,
						models.ScanValueType(resultValue),
						conf.SyncRules.OrgID,
						conf.SyncRules.AdminID,
						conf.SyncRules.AdminID)

					err := models.CreateContactField(ctx, db, cf)
					if err != nil {
						errMsg := fmt.Sprintf("error creating contact field: %v", v.FieldMapName)
						slog.Error(errMsg, "err", err)
						return updated, errors.Wrap(err, errMsg)
					} else {
						switch conf.Table.RelationType {
						case RelationTypeContact:
							err := models.UpdateContactField(ctx, db, contact, cf.UUID, resultValue)
							if err != nil {
								errMsg := fmt.Sprintf("error updating contact field: %v", v.FieldMapName)
								slog.Error(errMsg, "err", err)
								return updated, errors.Wrap(err, errMsg)
							}
						case RelationTypeURN:
							err := models.UpdateContactFieldByURN(ctx, db, contact, conf.SyncRules.OrgID, cf.UUID, resultValue)
							if err != nil {
								errMsg := fmt.Sprintf("error updating contact field: %v", v.FieldMapName)
								slog.Error(errMsg, "err", err)
								return updated, errors.Wrap(err, errMsg)
							}
						}
					}
				}
			}
		}
		updated++
	}
	return updated, nil
}

func DoQuery(s Syncer, currOffset, batchSize int, conditionList []string) ([]map[string]any, error) {
	query, err := s.GenerateSelectToSyncQuery(currOffset, batchSize, conditionList)
	if err != nil {
		slog.Error("error generating query", "err", err)
		return nil, errors.Wrap(err, "error generating query")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()
	results, err := s.MakeQuery(ctx, query)
	if err != nil {
		slog.Error("error executing query", "err", err)
		return nil, errors.Wrap(err, "error executing query")
	}
	return results, nil
}

func SyncContactFields(db *sqlx.DB, s Syncer) (int, error) {
	var startTime = time.Now()
	var updated int
	var updatedMutex sync.Mutex
	conf := configs.GetConfig()
	batchSize := conf.BatchSize
	maxGoroutines := conf.MaxGoroutines
	totalRows, err := s.GetTotalRows()
	if err != nil {
		return 0, errors.Wrap(err, "failed to get total rows")
	}

	sem := semaphore.NewWeighted(int64(maxGoroutines))
	var eg errgroup.Group
	var count int
	var countMutex sync.Mutex

	for offset := 0; offset < totalRows; offset += batchSize {

		currOffset := offset

		results, err := DoQuery(s, currOffset, batchSize, nil)
		if err != nil {
			slog.Error("error querying results", "err", err)
			return updated, err
		}

		if err := sem.Acquire(context.TODO(), 1); err != nil {
			return 0, err
		}

		eg.Go(func() error {
			defer sem.Release(1)

			updatedPerformed, err := applySync(s.GetConfig(), db, results)
			updatedMutex.Lock()
			updated += updatedPerformed
			updatedMutex.Unlock()
			if err != nil {
				slog.Error("error executing sync", "err", err)
				return errors.Wrap(err, "error executing sync")
			}

			countMutex.Lock()
			count += len(results)
			percentage := float64(count) / float64(totalRows) * 100

			// Calculate percentage completion and elapsed time
			elapsedTime := time.Since(startTime)
			estimatedTimeRemaining := (elapsedTime / time.Duration(count)) * time.Duration(totalRows-count)
			slog.Info(fmt.Sprintf("%s(%s) Queried count: %d, Total Rows: %d", s.GetConfig().ID, s.GetConfig().Service.Name, count, totalRows))
			slog.Info(fmt.Sprintf("%s(%s) Progress: %.2f%%, Elapsed time: %s, Estimated remaining time: %s\n", s.GetConfig().ID, s.GetConfig().Service.Name, percentage, elapsedTime, estimatedTimeRemaining))
			countMutex.Unlock()

			return nil
		})

		if batchSize == 0 {
			break
		}
	}

	if err := eg.Wait(); err != nil {
		slog.Error("Error waiting for sync", "err", err)
		log.Fatal(err)
	}

	return updated, nil
}

func SyncContactFieldsStrategy2(ctx context.Context, db *sqlx.DB, s Syncer) (int, error) {
	var startTime = time.Now()
	var updated int
	var updatedMutex sync.Mutex
	conf := configs.GetConfig()
	batchSize := conf.BatchSize
	maxGoroutines := conf.MaxGoroutines
	contacts, err := models.GetContactsURNPathByOrgID(ctx, db, s.GetConfig().SyncRules.OrgID, s.GetConfig().SyncRules.Schema)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get contacts")
	}
	totalRows := len(contacts)

	sem := semaphore.NewWeighted(int64(maxGoroutines))
	var eg errgroup.Group
	var count int
	var countMutex sync.Mutex

	for offset := 0; offset < totalRows; offset += batchSize {
		contactsToQuery := contacts[offset:]

		results, err := DoQuery(s, 0, 0, contactsToQuery)
		if err != nil {
			slog.Error("error querying results", "err", err)
			return updated, err
		}

		if err := sem.Acquire(context.TODO(), 1); err != nil {
			return 0, err
		}

		eg.Go(func() error {
			defer sem.Release(1)

			updatedPerformed, err := applySync(s.GetConfig(), db, results)
			updatedMutex.Lock()
			updated += updatedPerformed
			updatedMutex.Unlock()
			if err != nil {
				slog.Error("error executing sync", "err", err)
				return errors.Wrap(err, "error executing sync")
			}

			countMutex.Lock()
			count += len(results)
			percentage := float64(count) / float64(totalRows) * 100

			// Calculate percentage completion and elapsed time
			elapsedTime := time.Since(startTime)
			estimatedTimeRemaining := (elapsedTime / time.Duration(count)) * time.Duration(totalRows-count)
			slog.Info(fmt.Sprintf("%s(%s) Queried count: %d, Total Rows: %d", s.GetConfig().ID, s.GetConfig().Service.Name, count, totalRows))
			slog.Info(fmt.Sprintf("%s(%s) Progress: %.2f%%, Elapsed time: %s, Estimated remaining time: %s\n", s.GetConfig().ID, s.GetConfig().Service.Name, percentage, elapsedTime, estimatedTimeRemaining))
			countMutex.Unlock()

			return nil
		})

		if batchSize == 0 {
			break
		}
	}

	if err := eg.Wait(); err != nil {
		slog.Error("Error waiting for sync", "err", err)
		log.Fatal(err)
	}

	return updated, nil
}

func StringStructToMap(st string) map[string]string {
	str := strings.TrimPrefix(st, "{")
	str = strings.TrimPrefix(str, "}")

	pairs := strings.Split(str, ", ")

	data := make(map[string]string)

	for _, pair := range pairs {
		keyValue := strings.Split(pair, "=")
		if len(keyValue) == 2 {
			key := keyValue[0]
			value := keyValue[1]
			data[key] = value
		}
	}

	return data
}
