# Database Migration Tool for R2DBC-based applications

## Requirements

- Java 11 or higher

## Examples

#### Create new migrator instance
```java
Migrator migrator = new Migrator.Builder("r2dbc:postgresql://host:port/database", "username", "password")
        .withResourcePath("/some/path/to/your/folder")
        .build();
```

#### Contents of migration folder
```
|> /some/path/to/your/folder
  |> V1__first_migration.sql
  |> V2__second_migration.sql
  |> V3__third_migration.sql
```

#### Applying migration
```java
migrator.migrate();
```
