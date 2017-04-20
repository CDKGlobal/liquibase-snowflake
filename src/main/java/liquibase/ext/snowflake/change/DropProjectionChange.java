package liquibase.ext.snowflake.change;

import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.ext.snowflake.database.SnowflakeDatabase;
import liquibase.ext.snowflake.statement.DropProjectionStatement;
import liquibase.statement.SqlStatement;

/**
 * Created with IntelliJ IDEA.
 * User: vesterma
 * Date: 11/11/13
 * Time: 14:13
 * To change this template use File | Settings | File Templates.
 */
@DatabaseChange(name="dropProjection", description = "Drops an existing projection", priority = ChangeMetaData.PRIORITY_DEFAULT, appliesTo = "projection")
public class DropProjectionChange extends AbstractChange {
    private String schemaName;
    private String tableName;
    private String projectionName;
    private Boolean cascade;

    public Boolean getCascade() {
        return cascade;
    }

    public void setCascade(Boolean cascade) {
        this.cascade = cascade;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getProjectionName() {
        return projectionName;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        return new SqlStatement[]{
                new DropProjectionStatement(getSchemaName(), getTableName(), getProjectionName(),getCascade())
        };
    }

    @Override
    public String getConfirmationMessage() {
        return "Projection " + getProjectionName() + " dropped";
    }

    public void setProjectionName(String projectionName) {
        this.projectionName = projectionName;
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof SnowflakeDatabase;
    }
}
