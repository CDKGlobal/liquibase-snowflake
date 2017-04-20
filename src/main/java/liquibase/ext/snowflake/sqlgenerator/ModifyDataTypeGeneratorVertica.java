package liquibase.ext.snowflake.sqlgenerator;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.ext.snowflake.database.SnowflakeDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.ModifyDataTypeGenerator;
import liquibase.statement.core.ModifyDataTypeStatement;

/**
 * Created by vesterma on 20/02/14.
 */
public class ModifyDataTypeGeneratorVertica extends ModifyDataTypeGenerator {


    @Override
    public Sql[] generateSql(ModifyDataTypeStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String alterTable = "ALTER TABLE " + database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName());

        // add "MODIFY"
        alterTable += " " + getModifyString(database) + " ";

        // add column name
        alterTable += database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), statement.getColumnName());

        alterTable += getPreDataTypeString(database); // adds a space if nothing else

        // add column type
        alterTable += DataTypeFactory.getInstance().fromDescription(statement.getNewDataType(),database).toDatabaseDataType(database);

        return new Sql[]{new UnparsedSql(alterTable, getAffectedTable(statement))};
    }


    /**
     * @return either "MODIFY" or "ALTER COLUMN" depending on the current com.hp.db
     */
    protected String getModifyString(Database database) {
        return "ALTER COLUMN";
    }

    /**
     * @return the string that comes before the column type
     *         definition (like 'set data type' for derby or an open parentheses for Oracle)
     */
    protected String getPreDataTypeString(Database database) {
        return " SET DATA TYPE ";
    }

    @Override
    public boolean supports(ModifyDataTypeStatement statement, Database database) {
        return database instanceof SnowflakeDatabase;

    }
}
