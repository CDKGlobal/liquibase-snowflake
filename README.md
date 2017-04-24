# liquibase-snowflake

Liquibase extension to add Snowflake support

## Database

Significant overrides

### currentDateTimeFunction

Snowflake's `current_timestamp` function returns a `timestamp_ltz` datetype, while
the `datetime` Liquibase datatype maps to a Snowflake `timestamp_ntz` column.  To avoid exceptions, the current_timestamp
 is cast to a `timestamp_ntz`.   Without the cast, exceptions of the form given below occur.

    SQL compilation error: Expression type does not match column data type, expecting TIMESTAMP_NTZ(9) but got TIMESTAMP_LTZ(9)

### getJdbcCatalogName

The Snowflake JDBC drivers implementation of `DatabaseMetadata.getTables()` hard codes quotes around the catalog, schema and
table names, resulting in queries of the form:

    show tables like 'DATABASECHANGELOG' in schema "sample_db"."samnple_schema"

This results in the `DATABASECHANGELOG` table not being found, even after it has been created.  Since Snowflake stores
 catalog and schema names in upper case, the getJdbcCatalogName returns an upper case value. 

### getJdbcSchemaName

See [getJdbcCatalogName](#getJdbcCatalogName)

## Datatype Mappings

### datetime

The `datetime` datatype in Snowflake is an alias for the datatype `timestamp_ntz`, [Date and Time Data Types](https://docs.snowflake.net/manuals/sql-reference/data-types.html#date-and-time-data-types).
The `TimestampNTZType` class clarifies this mapping from Liquibase `datetime` to Snowflake `timestamp_ntz`.
