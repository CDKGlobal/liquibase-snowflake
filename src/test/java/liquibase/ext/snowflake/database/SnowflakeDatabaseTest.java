package liquibase.ext.snowflake.database;

import liquibase.ext.snowflake.helpers.SetUtils;
import org.junit.Before;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;

import static liquibase.servicelocator.PrioritizedService.PRIORITY_DATABASE;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class SnowflakeDatabaseTest {

    SnowflakeDatabase database;

    @Before
    public void setup() {
        database = new SnowflakeDatabase();
    }

    @Test
    public void testGetShortName() {
        assertEquals("snowflake", database.getShortName());
    }

    @Test
    public void testGetDefaultDatabaseProductName() {
        assertEquals("Snowflake Database", database.getDefaultDatabaseProductName());
    }

    @Test
    public void testGetDefaultPort() {
        assertNull(database.getDefaultPort());
    }

    @Test
    public void testGetCurrentTimeFunction() {
        assertEquals("current_timestamp", database.getCurrentDateTimeFunction());
    }

    @Test
    public void testGetSystemTables() {
        assertThat(SetUtils.equals(new HashSet<String>(), database.getSystemViews()), is(true));
    }

    @Test
    public void testGetSystemViews() {
        assertThat(SetUtils.equals(new HashSet<String>(), database.getSystemViews()), is(true));
    }

    @Test
    public void testGetPriority() {
        assertEquals(PRIORITY_DATABASE, database.getPriority());
    }

    @Test
    public void testSupportsInitiallyDeferrableColumns() {
        assertFalse(database.supportsInitiallyDeferrableColumns());
    }

    @Test
    public void testSupportsDropTableCascadeConstraints() {
        assertTrue(database.supportsDropTableCascadeConstraints());
    }

    //TODO - test isCorrectDatabaseImplementation

    @Test
    public void testGetDefaultDriver() {
        assertEquals("net.snowflake.client.jdbc.SnowflakeDriver", database.getDefaultDriver("jdbc:snowflake:"));
        assertNull(database.getDefaultDriver("jdbc:wrong-name:"));
    }

    @Test
    public void testSupportsSchemas() {
        assertTrue(database.supportsSchemas());
    }

    @Test
    public void testSupportsCatalogs() {
        assertFalse(database.supportsCatalogs());
    }

    @Test
    public void testSupportsCatalogInObjectName() {
        assertFalse(database.supportsCatalogInObjectName(null));
    }

    @Test
    public void testSupportsSequences() {
        assertTrue(database.supportsSequences());
    }

    @Test
    public void testGetDatabaseChangeLogTableName() {
        assertEquals("DATABASECHANGELOG", database.getDatabaseChangeLogTableName());
    }

    @Test
    public void testGetDatabaseChangeLogLockTableName() {
        assertEquals("DATABASECHANGELOGLOCK", database.getDatabaseChangeLogLockTableName());
    }

    //TODO - this seems to just be testing method in abstract class
    @Test
    public void testIsSystemObject() {
        assertFalse(database.isSystemObject(null));
    }

    @Test
    public void testSupportsTablespaces() {
        assertFalse(database.supportsTablespaces());
    }

    @Test
    public void testSupportsAutoIncrementClause() {
        assertTrue(database.supportsAutoIncrement());
    }

    @Test
    public void testGetAutoIncrementClause() {
        assertEquals("", database.getAutoIncrementClause());
        assertEquals(" AUTOINCREMENT ", database.getAutoIncrementClause(null, null));
        assertEquals(" AUTOINCREMENT(1,1) ", database.getAutoIncrementClause(new BigInteger("1"), new BigInteger("1")));
    }

    @Test
    public void testGenerateAutoIncrementStartWith() {
        assertTrue(database.generateAutoIncrementStartWith(new BigInteger("1")));
    }

    @Test
    public void testGenerateAutoIncrementBy() {
        assertTrue(database.generateAutoIncrementBy(new BigInteger("1")));
    }

    @Test
    public void testSupportsRestrictFoeignKeys() {
        assertTrue(database.supportsRestrictForeignKeys());
    }

    @Test
    public void testIsReservedWord() {
        database.addReservedWords(Arrays.asList("TABLE", "FROM", "INTO"));
        assertTrue(database.isReservedWord("table"));
    }

}