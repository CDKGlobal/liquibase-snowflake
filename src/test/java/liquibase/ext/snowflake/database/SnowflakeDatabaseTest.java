package liquibase.ext.snowflake.database;

import liquibase.database.AbstractJdbcDatabase;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SnowflakeDatabaseTest {

    AbstractJdbcDatabase database;

    @Before
    public void setup() {
        database = new SnowflakeDatabase();
    }

    @Test
    public void testShortName() {
        assertEquals("snowflake", database.getShortName());
    }

}