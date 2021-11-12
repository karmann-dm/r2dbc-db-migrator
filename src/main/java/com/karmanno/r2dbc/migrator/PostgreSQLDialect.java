package com.karmanno.r2dbc.migrator;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Statement;

public class PostgreSQLDialect implements DatabaseDialect {
    @Override
    public Statement checkTableExists(Connection connection, String tableName) {
        return connection.createStatement(
                "select exists (select from pg_tables where schemaname = 'public' and tablename = 'migration_log')"
        );
    }

    @Override
    public Statement createMigrationTable(Connection connection) {
        return connection.createStatement(
                "create table migration_log (id bigserial primary key, name varchar(255) not null unique, created_at bigint not null)"
        );
    }

    @Override
    public Statement checkMigrationExists(Connection connection, String migrationName) {
        return connection.createStatement(
                "select exists (select name from migration_log where name = 'v1__first_migration.sql');"
        );
    }

    @Override
    public Statement insertConfirmStatement(Connection connection, String migrationName) {
        return connection.createStatement(
                "insert into migration_log (created_at, name) values (" + System.currentTimeMillis() + ", '" + migrationName + "');"
        );
    }
}
