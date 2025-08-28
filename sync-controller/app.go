package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/pg"
)

type Context struct {
	config      *Config
	dbpool      *pgxpool.Pool
	jobRunner   *JobRunner
	taskManager *TaskManager
	server      *http.Server
}

func (a *Context) InitContext(settings *appbase.AppSettings) error {
	var err error
	a.config = &Config{}
	err = appbase.InitAppConfig(a.config, settings)
	if err != nil {
		return err
	}
	a.dbpool, err = pg.NewPGPool(a.config.DatabaseURL)
	if err != nil {
		return fmt.Errorf("Unable to create postgres connection pool: %v\n", err)
	}
	err = InitDBSchema(a.dbpool)
	if err != nil {
		return err
	}
	a.jobRunner, err = NewJobRunner(a)
	if err != nil {
		return err
	}
	a.taskManager, err = NewTaskManager(a)

	router := NewRouter(a)
	a.server = &http.Server{
		Addr:              fmt.Sprintf("0.0.0.0:%d", a.config.HTTPPort),
		Handler:           router.Engine(),
		ReadTimeout:       time.Second * 60,
		ReadHeaderTimeout: time.Second * 60,
		IdleTimeout:       time.Second * 65,
	}
	return nil
}

func (a *Context) Cleanup() error {
	a.taskManager.Close()
	a.jobRunner.Close()
	a.dbpool.Close()
	return nil
}

func (a *Context) ShutdownSignal() error {
	_ = a.server.Shutdown(context.Background())
	return nil
}

func (a *Context) Server() *http.Server {
	return a.server
}

func (a *Context) Config() *Config {
	return a.config
}
