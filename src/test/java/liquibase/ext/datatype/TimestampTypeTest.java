package liquibase.ext.datatype;

import liquibase.database.core.PostgresDatabase;
import liquibase.datatype.DataTypeFactory;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.ext.database.SnowflakeDatabase;
import org.junit.Before;
import org.junit.Test;

import static liquibase.servicelocator.PrioritizedService.PRIORITY_DATABASE;
import static org.hamcrest.collection.IsArrayContaining.hasItemInArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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
    public void timestampAliases() throws Exception {
        LiquibaseDataType liquibaseDataType = DataTypeFactory.getInstance().fromDescription("datetime", snowflakeDatabase);
        String[] aliases = liquibaseDataType.getAliases();
        assertEquals(2, aliases.length);
        assertThat(aliases, hasItemInArray("datetime"));
        assertThat(aliases, hasItemInArray("java.sql.Types.DATETIME"));
    }

    @Test
    public void datetimeConvertsToTimestamp() throws Exception {
        LiquibaseDataType liquibaseDataType = DataTypeFactory.getInstance().fromDescription("datetime", snowflakeDatabase);
        assertEquals("liquibase.ext.datatype.TimestampType", liquibaseDataType.getClass().getName());
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

    @Test
    public void getMinParameters() throws Exception {
        assertEquals(0, timestampType.getMinParameters(snowflakeDatabase));
    }

    @Test
    public void getMaxParameters() throws Exception {
        assertEquals(0, timestampType.getMinParameters(snowflakeDatabase));
    }

}