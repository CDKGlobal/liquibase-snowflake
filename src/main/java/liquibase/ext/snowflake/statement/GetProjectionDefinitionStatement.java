package liquibase.ext.snowflake.statement;

import liquibase.statement.AbstractSqlStatement;

/**
 * Created with IntelliJ IDEA.
 * User: vesterma
 * Date: 27/11/13
 * Time: 13:14
 * To change this template use File | Settings | File Templates.
 */
public class GetProjectionDefinitionStatement  extends AbstractSqlStatement {
    private String catalogName;
    private String schemaName;
    private String projectionName;

    public GetProjectionDefinitionStatement(String catalogName, String schemaName, String projectionName) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.projectionName = projectionName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getProjectionName() {
        return projectionName;
    }
}

