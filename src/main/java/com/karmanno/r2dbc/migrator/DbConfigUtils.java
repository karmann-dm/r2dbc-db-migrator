package com.karmanno.r2dbc.migrator;

import java.net.URI;
import java.util.Objects;

class DbConfigUtils {
    private static final String URL_PREFIX = "r2dbc:";

    public static String[] parseUrl(String url) {
        if (!url.startsWith(URL_PREFIX)) {
            throw new R2DBCMigratorException("DB Url should start from r2dbc");
        }
        String cleanUri = url.substring(URL_PREFIX.length());
        URI uri = URI.create(cleanUri);

        String[] result = new String[4];
        result[0] = uri.getScheme();
        result[1] = uri.getHost();
        result[2] = Objects.toString(uri.getPort());
        result[3] = uri.getPath().replace("/", "");

        return result;
    }
}
