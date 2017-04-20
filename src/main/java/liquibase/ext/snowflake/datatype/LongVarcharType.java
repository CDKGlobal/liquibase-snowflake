package liquibase.ext.snowflake.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.ext.snowflake.database.SnowflakeDatabase;
import liquibase.util.StringUtils;

/**
 * Created by vesterma on 23/01/14.
 */

@DataTypeInfo(name="long varchar", aliases = { "java.sql.Types.LONGNVARCHAR", "text", "clob","LONG VARCHAR"}, minParameters = 0, maxParameters = 0, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class LongVarcharType extends LiquibaseDataType {
    private String originalDefinition;

    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {
        String originalDefinition = StringUtils.trimToEmpty(this.originalDefinition);

        if (database instanceof SnowflakeDatabase) {
            return new DatabaseDataType("LONG VARCHAR", getParameters());
        }

        return super.toDatabaseDataType(database);
    }
    @Override
    public void finishInitialization(String originalDefinition) {
        super.finishInitialization(originalDefinition);
        this.originalDefinition = originalDefinition;
    }

    @Override
    public boolean supports(Database database) {
        if (database instanceof SnowflakeDatabase)
            return true;
        return false;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }
}

