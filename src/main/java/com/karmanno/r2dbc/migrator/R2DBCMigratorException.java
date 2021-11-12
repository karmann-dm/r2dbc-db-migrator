package com.karmanno.r2dbc.migrator;

public class R2DBCMigratorException extends RuntimeException {
    public R2DBCMigratorException(String message) {
        super(message);
    }

    public R2DBCMigratorException(String message, Throwable e) {
        super(message, e);
    }
}
