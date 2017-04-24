package liquibase.ext.database;

import liquibase.CatalogAndSchema;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.logging.LogFactory;
import liquibase.structure.DatabaseObject;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class SnowflakeDatabase extends AbstractJdbcDatabase {

    public static final String PRODUCT_NAME = "Snowflake";

    private Set<String> systemTables = new HashSet<String>();
    private Set<String> systemViews = new HashSet<String>();
    private Set<String> reservedWords = new HashSet<String>();

    public SnowflakeDatabase() {
        super.setCurrentDateTimeFunction("current_timestamp");
        super.unmodifiableDataTypes.addAll(Arrays.asList("integer", "bool", "boolean", "int4", "int8", "float4", "float8", "numeric", "bigserial", "serial", "bytea", "timestamptz"));
        super.unquotedObjectsAreUppercased = false;
    }

    @Override
    public String getShortName() {
        return "snowflake";
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return PRODUCT_NAME;
    }

    @Override
    public Integer getDefaultPort() {
        return null;
    }

    @Override
    public Set<String> getSystemTables() {
        return systemTables;
    }

    @Override
    public Set<String> getSystemViews() {
        return systemViews;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supportsInitiallyDeferrableColumns() {
        return false;
    }

    @Override
    public boolean supportsDropTableCascadeConstraints() {
        return true;
    }

    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        return PRODUCT_NAME.equalsIgnoreCase(conn.getDatabaseProductName());
    }

    @Override
    public String getDefaultDriver(String url) {
        if (url.startsWith("jdbc:snowflake:")) {
            return "net.snowflake.client.jdbc.SnowflakeDriver";
        }
        return null;
    }

    @Override
    public String getDefaultCatalogName() {
        return super.getDefaultCatalogName().toUpperCase();
    }

    @Override
    public String getDefaultSchemaName() {
        return super.getDefaultSchemaName().toUpperCase();
    }

    @Override
    public String getJdbcCatalogName(final CatalogAndSchema schema) {
        return super.getJdbcCatalogName(schema).toUpperCase();
    }

    @Override
    public String getJdbcSchemaName(final CatalogAndSchema schema) {
        return super.getJdbcSchemaName(schema).toUpperCase();
    }

    @Override
    public boolean supportsCatalogs() {
        return true;
    }

    @Override
    public boolean supportsCatalogInObjectName(Class<? extends DatabaseObject> type) {
        return false;
    }

    @Override
    public boolean supportsSequences() {
        return true;
    }

    @Override
    public String getDatabaseChangeLogTableName() {
        return super.getDatabaseChangeLogTableName().toUpperCase();
    }

    @Override
    public String getDatabaseChangeLogLockTableName() {
        return super.getDatabaseChangeLogLockTableName().toUpperCase();
    }

    @Override
    public boolean isSystemObject(DatabaseObject example) {
        return super.isSystemObject(example);
    }

    @Override
    public boolean supportsTablespaces() {
        return false;
    }

    @Override
    public String getAutoIncrementClause(BigInteger startWith, BigInteger incrementBy) {
        if (startWith != null && incrementBy != null) {
            return " AUTOINCREMENT(" + startWith + "," + incrementBy + ") ";
        }
        return " AUTOINCREMENT ";
    }

    @Override
    public boolean supportsAutoIncrement() {
        return true;
    }

    @Override
    public String getAutoIncrementClause() {
        return "";
    }

    @Override
    public boolean generateAutoIncrementStartWith(BigInteger startWith) {
        return true;
    }

    @Override
    public boolean generateAutoIncrementBy(BigInteger incrementBy) {
        return true;
    }

    @Override
    public boolean supportsRestrictForeignKeys() {
        return true;
    }

    @Override
    public void addReservedWords(Collection<String> words) {
        reservedWords.addAll(words);
    }

    @Override
    public boolean isReservedWord(String tableName) {
        return reservedWords.contains(tableName.toUpperCase());
    }

    @Override
    protected String getConnectionSchemaName() {
        DatabaseConnection connection = getConnection();
        if (connection == null) {
            return null;
        }
        try {
            ResultSet resultSet = ((JdbcConnection) connection).createStatement().executeQuery("SELECT CURRENT_SCHEMA()");
            resultSet.next();
            String schema = resultSet.getString(1);
            return schema;
        } catch (Exception e) {
            LogFactory.getLogger().info("Error getting default schema", e);
        }
        return null;
    }

    public String executeSQL(String query) {
        DatabaseConnection connection = getConnection();
        if (connection == null) {
            return null;
        }
        StringBuilder res = null;
        try {
            ResultSet resultSet = ((JdbcConnection) connection).createStatement().executeQuery(query);
            while (resultSet.next()) {
                if (res == null) {
                    res = new StringBuilder();
                }
                res.append(resultSet.getString(1));
            }
            if (res != null)
                return res.toString();
        } catch (Exception e) {
            LogFactory.getLogger().info("Error got exception when running: " + query, e);
        }
        return null;
    }

    @Override
    public boolean supportsSchemas() {
        return true;
    }
}
