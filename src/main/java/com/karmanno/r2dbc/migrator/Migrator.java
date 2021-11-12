package com.karmanno.r2dbc.migrator;

import io.netty.util.internal.StringUtil;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class Migrator {
    private static final Logger logger = LoggerFactory.getLogger(Migrator.class);
    private static final Pattern FILENAME_VERSION_PATTERN = Pattern.compile("V(\\d+)__.*");

    private final Mono<? extends Connection> connection;
    private final DatabaseDialect dialect;
    private final Path resourcePath;

    private Migrator(Mono<? extends Connection> connection,
                     ConnectionFactoryOptions options,
                     Path resourcePath) {
        this.connection = connection;
        this.resourcePath = resourcePath;

        DatabaseDialectResolver resolver = new DatabaseDialectResolver();
        this.dialect = resolver.resolveDialect(options.getValue(ConnectionFactoryOptions.DRIVER));
    }

    private int extractVersionFromPath(Path path) {
        var filename = path.getFileName().toString();
        var matcher = FILENAME_VERSION_PATTERN.matcher(filename);
        if (!matcher.find()) {
            throw new R2DBCMigratorException(filename + " should be in format V[version]__description.sql");
        }
        return Integer.parseInt(matcher.group(1));
    }

    private List<Path> makeExecutionList() {
        if (!Files.exists(resourcePath)) {
            throw new R2DBCMigratorException("Couldn't find resource path: " + resourcePath);
        }
        if (!Files.isDirectory(resourcePath)) {
            throw new R2DBCMigratorException("Resource path should be a directory");
        }

        try (Stream<Path> walk = Files.walk(resourcePath)) {
            return walk.filter(path -> com.google.common.io.Files.getFileExtension(path.getFileName().toString()).equalsIgnoreCase("sql"))
                    .map(path -> new VersionTransport(extractVersionFromPath(path), path))
                    .sorted(Comparator.comparingInt(VersionTransport::getVersion))
                    .map(VersionTransport::getPath)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new R2DBCMigratorException("Couldn't create execution list", e);
        }
    }

    private void checkMigrationLogTable() {
        var exists = connection.flatMap(c -> Mono.from(
                dialect.checkTableExists(c, "migration_log").execute()
        ).map(result -> result.map((row, rowMetadata) -> row.get("exists", Boolean.class))).flatMap(Mono::from)).block();

        if (Boolean.FALSE.equals(exists)) {
            connection.flatMap(c -> Mono.from(
                    dialect.createMigrationTable(c).execute()
            )).block();
        }
    }

    private boolean migrationApplied(String name) {
        var exists = connection.flatMap(c -> Mono.from(
                dialect.checkMigrationExists(c, name).execute()
        ).map(result -> result.map((row, rowMetadata) -> row.get("exists", Boolean.class))).flatMap(Mono::from)).block();

        return Objects.requireNonNullElse(exists, false);
    }

    public void migrate() {
        logger.info("Start migrating...");

        checkMigrationLogTable();

        var filesToExecute = makeExecutionList();
        for (Path path : filesToExecute) {
            try {
                String migrationName = path.getFileName().toString().toLowerCase();
                String content = Files.readString(path);

                if (!migrationApplied(migrationName)) {
                    AtomicBoolean finishedWithError = new AtomicBoolean(false);
                    AtomicReference<Throwable> throwableAtomicReference = new AtomicReference<>();
                    connection.flatMap(c -> Mono.from(c.beginTransaction())
                            .then(Mono.from(c.createStatement(content).execute()))
                            .then(Mono.from(dialect.insertConfirmStatement(c, migrationName).execute()))
                            .doOnError(e -> { finishedWithError.set(true); throwableAtomicReference.set(e); })
                            .delayUntil(r -> c.commitTransaction())
                    ).block();

                    if (finishedWithError.get()) {
                        throw new R2DBCMigratorException("Migration " + migrationName + " failed with exception", throwableAtomicReference.get());
                    }

                    logger.info("Applied migration: {}", migrationName);
                } else {
                    logger.info("Migration {} is already applied, skipping...", migrationName);
                }

            } catch (IOException e) {
                throw new R2DBCMigratorException("Couldn't read migration contents from the file", e);
            }
        }

        logger.info("Migration finished successfully!");
    }

    public static class Builder {
        private final ConnectionFactory connectionFactory;
        private String resourcePath;
        private ConnectionFactoryOptions options;

        public Builder(String databaseUrl, String username, String password) {
            ConnectionFactoryOptions baseOptions = ConnectionFactoryOptions.parse(databaseUrl);
            ConnectionFactoryOptions.Builder builder = ConnectionFactoryOptions.builder().from(baseOptions);
            if (!StringUtil.isNullOrEmpty(username)) {
                builder.option(ConnectionFactoryOptions.USER, username);
            }
            if (!StringUtil.isNullOrEmpty(password)) {
                builder.option(ConnectionFactoryOptions.PASSWORD, password);
            }
            this.options = builder.build();
            this.connectionFactory = ConnectionFactories.get(this.options);
        }

        public Builder(ConnectionFactory connectionFactory) {
            this.connectionFactory = connectionFactory;
        }

        public Builder withResourcePath(String resourcePath) {
            this.resourcePath = resourcePath;
            return this;
        }

        public Migrator build() {
            var connectionMono = Mono.from(connectionFactory.create());
            return new Migrator(connectionMono, options, Paths.get(resourcePath));
        }
    }

    private static class VersionTransport {
        private final int version;
        private final Path path;

        public VersionTransport(int version, Path path) {
            this.version = version;
            this.path = path;
        }

        public int getVersion() {
            return version;
        }

        public Path getPath() {
            return path;
        }
    }
}
