package syncer

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/pkg/errors"
	"github.weni-ai/flows-field-syncer/configs"
)

type SyncerAPI struct {
	SyncerConfRepo SyncerConfRepository
	Server         *echo.Echo
	Config         *configs.Config
}

func NewSyncerAPI(config *configs.Config, syncerConfRepo SyncerConfRepository) *SyncerAPI {
	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	api := &SyncerAPI{
		SyncerConfRepo: syncerConfRepo,
		Server:         e,
		Config:         config,
	}
	api.setupSyncerConfRoutes()

	return api
}

func (a *SyncerAPI) Start() {
	go func() {
		if err := a.Server.Start(a.Config.HostAPI + a.Config.PortAPI); err != nil && err != http.ErrServerClosed {
			a.Server.Logger.Fatal(errors.Wrap(err, "shutting down the api server"))
		}
	}()
}

func (a *SyncerAPI) createSyncerConfHandler(c echo.Context) error {
	syncerConf := new(SyncerConf)
	if err := c.Bind(syncerConf); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}
	err := a.SyncerConfRepo.Create(*syncerConf)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusCreated, syncerConf)
}

func (a *SyncerAPI) getSyncerConfHandler(c echo.Context) error {
	id := c.Param("id")
	syncerConf, err := a.SyncerConfRepo.GetByID(id)
	if err != nil {
		return c.JSON(http.StatusNotFound, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, syncerConf)
}

func (a *SyncerAPI) updateSyncerConfHandler(c echo.Context) error {
	id := c.Param("id")

	syncerConf := new(SyncerConf)
	if err := c.Bind(syncerConf); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}
	err := a.SyncerConfRepo.Update(id, *syncerConf)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}

func (a *SyncerAPI) deleteSyncerConfHandler(c echo.Context) error {
	id := c.Param("id")
	err := a.SyncerConfRepo.Delete(id)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.NoContent(http.StatusNoContent)
}

var confPath = "/config"

func (a *SyncerAPI) setupSyncerConfRoutes() {
	a.Server.POST(confPath, a.createSyncerConfHandler)
	a.Server.GET(confPath+"/:id", a.getSyncerConfHandler)
	a.Server.PUT(confPath+"/:id", a.updateSyncerConfHandler)
	a.Server.DELETE(confPath+"/:id", a.deleteSyncerConfHandler)
}
