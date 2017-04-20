package liquibase.ext.snowflake.statement;

import liquibase.statement.AbstractSqlStatement;

/**
 * Created with IntelliJ IDEA.
 * User: vesterma
 * Date: 11/11/13
 * Time: 14:16
 * To change this template use File | Settings | File Templates.
 */
public class DropProjectionStatement extends AbstractSqlStatement {

    private String projectionName;
    private String schemaName;
    private String tableName;
    private boolean cascade;


    public DropProjectionStatement(String schemaName, String tableName, String projectionName, boolean cascade) {
        this.projectionName = projectionName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.cascade = cascade;

    }

    public String getProjectionName() {
        return projectionName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isCascade() {
        return cascade;
    }

}
