package syncer

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.weni-ai/flows-field-syncer/configs"
)

type SyncerAPI struct {
	SyncerConfRepo  SyncerConfRepository
	Server          *echo.Echo
	Config          *configs.Config
	SyncerScheduler SyncerScheduler
}

func NewSyncerAPI(config *configs.Config, syncerConfRepo SyncerConfRepository, syncerScheduler SyncerScheduler) *SyncerAPI {
	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	api := &SyncerAPI{
		SyncerConfRepo:  syncerConfRepo,
		Server:          e,
		Config:          config,
		SyncerScheduler: syncerScheduler,
	}
	api.setupSyncerConfRoutes()

	return api
}

func (a *SyncerAPI) Start() {
	go func() {
		if err := a.Server.Start(a.Config.HostAPI + a.Config.PortAPI); err != nil && err != http.ErrServerClosed {
			slog.Error("error on start api server", "err", err)
			os.Exit(1)
		}
	}()
}

func (a *SyncerAPI) createSyncerConfHandler(c echo.Context) error {
	syncerConf := new(SyncerConf)
	if err := c.Bind(syncerConf); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}
	if len(syncerConf.SyncRules.ScheduleTimes) <= 0 {
		syncerConf.SyncRules.ScheduleTimes = []string{time.Now().Format("15:04")}
	} else {
		for i, st := range syncerConf.SyncRules.ScheduleTimes {
			_, err := time.Parse("15:04", st)
			if err != nil {
				return c.JSON(http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("sync rule for schedule time is invalid for element %d: %v", i, st)})
			}
		}
	}
	newConf, err := a.SyncerConfRepo.Create(*syncerConf)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	err = a.SyncerScheduler.RegisterSyncer(newConf)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusCreated, newConf)
}

func (a *SyncerAPI) getSyncerConfHandler(c echo.Context) error {
	id := c.Param("id")
	if id != "" {
		syncerConf, err := a.SyncerConfRepo.GetByID(id)
		if err != nil {
			return c.JSON(http.StatusNotFound, map[string]string{"error": err.Error()})
		}
		return c.JSON(http.StatusOK, syncerConf)
	}

	orgID := c.QueryParam("org_id")
	if orgID != "" {
		syncerConfs, err := a.SyncerConfRepo.GetByOrgID(orgID)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		return c.JSON(http.StatusOK, syncerConfs)
	}

	syncerConfs, err := a.SyncerConfRepo.GetAll()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, syncerConfs)
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
	err = a.SyncerScheduler.UnregisterSyncer(*syncerConf)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	if syncerConf.IsActive {
		a.SyncerScheduler.RegisterSyncer(*syncerConf)
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
	a.Server.GET("/", func(c echo.Context) error {
		return c.NoContent(http.StatusOK)
	})
	a.Server.GET(confPath, a.getSyncerConfHandler)
	a.Server.POST(confPath, a.createSyncerConfHandler)
	a.Server.GET(confPath+"/:id", a.getSyncerConfHandler)
	a.Server.PUT(confPath+"/:id", a.updateSyncerConfHandler)
	a.Server.DELETE(confPath+"/:id", a.deleteSyncerConfHandler)
}
