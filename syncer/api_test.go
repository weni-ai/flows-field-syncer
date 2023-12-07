package syncer

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.weni-ai/flows-field-syncer/configs"
)

func TestSyncerAPI(t *testing.T) {
	config := &configs.Config{}

	confsFile := "./testdata/bigquery.json"
	syncerConfJSON, err := os.ReadFile(confsFile)
	assert.NoError(t, err)
	mockRepo := new(MockSyncerConfRepository)
	api := NewSyncerAPI(config, mockRepo)

	t.Run("Create", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, confPath, bytes.NewReader(syncerConfJSON))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		c := api.Server.NewContext(req, rec)

		mockRepo.On("Create", mock.AnythingOfType("SyncerConf")).Return(nil)
		err = api.createSyncerConfHandler(c)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusCreated, rec.Code)

		var response SyncerConf
		err = json.NewDecoder(rec.Body).Decode(&response)
		assert.NoError(t, err)
		assert.Equal(t, response.Service.Name, "pull bigquery")
	})

	t.Run("GetByID", func(t *testing.T) {
		dummyConf := SyncerConf{
			ID:      "604ace25-c7a1-44c6-b895-8e9e230b857d",
			Service: SyncerService{Name: "pull bigquery"},
		}

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		req.Header.Set("Content-Type", "application/json")
		c := api.Server.NewContext(req, rec)
		c.SetPath("/config/" + dummyConf.ID)
		c.SetParamNames("id")
		c.SetParamValues(dummyConf.ID)

		mockRepo.On("GetByID", dummyConf.ID).
			Return(dummyConf, nil)

		err = api.getSyncerConfHandler(c)
		assert.NoError(t, err)

		assert.Equal(t, http.StatusOK, rec.Code)

		var response SyncerConf
		err = json.NewDecoder(rec.Body).Decode(&response)
		assert.NoError(t, err)
		assert.Equal(t, response.Service.Name, "pull bigquery")
	})

	mockRepo.AssertExpectations(t)

}

func DecodeFileConf(filepath string) (*SyncerConf, error) {
	confsFile := "./testdata/bigquery.json"
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

type MockSyncerConfRepository struct {
	mock.Mock
}

func (m *MockSyncerConfRepository) Create(syncerConf SyncerConf) error {
	args := m.Called(syncerConf)
	return args.Error(0)
}

func (m *MockSyncerConfRepository) GetAll() ([]SyncerConf, error) {
	args := m.Called()
	return args.Get(0).([]SyncerConf), args.Error(1)
}

func (m *MockSyncerConfRepository) GetByID(id string) (SyncerConf, error) {
	args := m.Called(id)
	return args.Get(0).(SyncerConf), args.Error(1)
}

func (m *MockSyncerConfRepository) Update(id string, syncerConf SyncerConf) error {
	args := m.Called(id, syncerConf)
	return args.Error(0)
}

func (m *MockSyncerConfRepository) Delete(id string) error {
	args := m.Called(id)
	return args.Error(0)
}
