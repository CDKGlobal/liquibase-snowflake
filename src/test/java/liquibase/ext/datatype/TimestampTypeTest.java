package liquibase.ext.datatype;

import liquibase.database.core.PostgresDatabase;
import liquibase.datatype.DatabaseDataType;
import liquibase.ext.database.SnowflakeDatabase;
import org.junit.Before;
import org.junit.Test;

import static liquibase.servicelocator.PrioritizedService.PRIORITY_DATABASE;
import static org.junit.Assert.*;

public class TimestampTypeTest {

    TimestampType timestampType;
    SnowflakeDatabase snowflakeDatabase;

    @Before
    public void setup() {
        timestampType = new TimestampType();
        snowflakeDatabase = new SnowflakeDatabase();
    }

    @Test
    public void toDatabaseDataType() throws Exception {
        DatabaseDataType databaseDataType = timestampType.toDatabaseDataType(snowflakeDatabase);
        assertEquals("TIMESTAMP_LTZ", databaseDataType.getType());
        assertEquals("TIMESTAMP_LTZ", databaseDataType.toSql());
        assertFalse(databaseDataType.isAutoIncrement());

    }

    @Test
    public void supports() throws Exception {
        assertTrue(timestampType.supports(snowflakeDatabase));
        assertFalse(timestampType.supports(new PostgresDatabase()));
    }

    @Test
    public void getPriority() throws Exception {
        assertEquals(PRIORITY_DATABASE, timestampType.getPriority());

    }

}