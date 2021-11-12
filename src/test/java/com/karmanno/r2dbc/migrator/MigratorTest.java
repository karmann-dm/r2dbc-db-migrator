package com.karmanno.r2dbc.migrator;

import org.junit.jupiter.api.Test;

public class MigratorTest {
    @Test
    public void migrate() {
        var migrator = new Migrator.Builder("r2dbc:postgresql://localhost:5432/langu", "postgres", "qwe")
                .withResourcePath("/home/karmanno/Projects/Personal/r2dbc-db-migrator/src/test/resources/migration")
                .build();
        migrator.migrate();
    }
}
