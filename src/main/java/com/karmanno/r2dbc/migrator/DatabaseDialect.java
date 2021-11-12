package com.karmanno.r2dbc.migrator;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Statement;

public interface DatabaseDialect {
    Statement checkTableExists(Connection connection, String tableName);
    Statement createMigrationTable(Connection connection);
    Statement checkMigrationExists(Connection connection, String migrationName);
    Statement insertConfirmStatement(Connection connection, String migrationName);
}
