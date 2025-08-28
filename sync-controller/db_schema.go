package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// HANDLED BY prisma in console project

var ddls = []string{
	`create table if not exists source_spec()`,
	`alter table source_spec add column if not exists package varchar not null`,
	`alter table source_spec add column if not exists version varchar not null`,
	`alter table source_spec add column if not exists specs json`,
	`alter table source_spec add column if not exists timestamp timestamp with time zone`,
	`alter table source_spec add column if not exists error varchar`,
	`alter table source_spec add constraint source_spec_pkey primary key (package, version)`,
	`create table if not exists source_catalog()`,
	`alter table source_catalog add column if not exists package varchar not null`,
	`alter table source_catalog add column if not exists version varchar not null`,
	`alter table source_catalog add column if not exists key varchar not null`,
	`alter table source_catalog add column if not exists catalog json`,
	`alter table source_catalog add column if not exists timestamp timestamp with time zone`,
	`alter table source_catalog add column if not exists status varchar`,
	`alter table source_catalog add column if not exists description varchar`,
	`alter table source_catalog add constraint source_catalog_pkey primary key (package, version, key)`,
	`create table if not exists source_check()`,
	`alter table source_check add column if not exists package varchar not null`,
	`alter table source_check add column if not exists version varchar not null`,
	`alter table source_check add column if not exists key varchar not null`,
	`alter table source_check add column if not exists status varchar`,
	`alter table source_check add column if not exists description varchar`,
	`alter table source_check add column if not exists timestamp timestamp with time zone`,
	`alter table source_check add constraint source_check_pkey primary key (key)`,
	`create table if not exists source_state()`,
	`alter table source_state add column if not exists sync_id varchar not null`,
	`alter table source_state add column if not exists stream varchar not null`,
	`alter table source_state add column if not exists state json`,
	`alter table source_state add column if not exists timestamp timestamp with time zone`,
	`alter table source_state add constraint source_state_pkey primary key (sync_id)`,

	`create table if not exists source_task()`,
	`alter table source_task add column if not exists sync_id varchar not null`,
	`alter table source_task add column if not exists task_id varchar not null`,
	`alter table source_task add column if not exists package varchar not null`,
	`alter table source_task add column if not exists version varchar not null`,
	`alter table source_task add column if not exists started_at timestamp with time zone`,
	`alter table source_task add column if not exists updated_at timestamp with time zone`,
	`alter table source_task add column if not exists started_by json`,
	`alter table source_task add column if not exists status varchar`,
	`alter table source_task add column if not exists description varchar`,
	`alter table source_task add column if not exists metrics json`,
	`alter table source_task add column if not exists error varchar`,
	`alter table source_task add constraint source_task_pkey primary key (task_id)`,
	`create index if not exists source_task_sync_id_index on source_task (sync_id)`,
	`create index if not exists source_task_started_at_index on source_task (started_at desc)`,

	`create table if not exists task_log()`,
	`alter table task_log add column if not exists id varchar not null`,
	`alter table task_log add column if not exists level varchar not null`,
	`alter table task_log add column if not exists logger varchar not null`,
	`alter table task_log add column if not exists message varchar not null`,
	`alter table task_log add column if not exists sync_id varchar not null`,
	`alter table task_log add column if not exists task_id    varchar not null`,
	`alter table task_log add column if not exists timestamp timestamp with time zone`,
	`alter table task_log add constraint task_log_pk primary key (id)`,
	`create index if not exists task_log_sync_id_index on task_log (sync_id)`,
	`create index if not exists task_log_started_at_index on task_log (task_id)`,
	`create index if not exists task_log_started_at_index on task_log (timestamp desc)`,
}

func InitDBSchema(dbpool *pgxpool.Pool) error {
	for _, ddl := range ddls {
		_, err := dbpool.Exec(context.Background(), ddl)
		if err != nil && !IsConstraintExistsError(err) {
			return fmt.Errorf("Error running DDL query '%s': %v", ddl, err)
		}
	}
	return nil
}

func IsConstraintExistsError(err error) bool {
	return strings.Contains(err.Error(), "multiple primary keys for table")
}
