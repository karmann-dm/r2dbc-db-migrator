package com.karmanno.r2dbc.migrator;

import java.util.Map;

public class DatabaseDialectResolver {
    private static final Map<String, DatabaseDialect> DIALECTS = Map.of(
            "postgresql", new PostgreSQLDialect()
    );

    public DatabaseDialect resolveDialect(String driver) {
        var dialect = DIALECTS.get(driver);
        if (dialect == null) {
            throw new R2DBCMigratorException("No dialect for driver " + driver);
        }
        return dialect;
    }
}
