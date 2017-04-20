package liquibase.ext.snowflake.statement;

import liquibase.statement.ColumnConstraint;
import liquibase.statement.core.AddColumnStatement;

/**
 * Created by vesterma on 10/12/13.
 */
public class AddVerticaColumnStatement extends AddColumnStatement {
    private String encoding;
    private Integer accessrank;

    public AddVerticaColumnStatement(String catalogName, String schemaName, String tableName, String columnName, String columnType, Object defaultValue, String encoding,Integer accessrank, ColumnConstraint... constraints) {
        super(catalogName, schemaName, tableName, columnName, columnType, defaultValue, constraints);
        this.encoding = encoding;
        this.accessrank = accessrank;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public Integer getAccessrank() {
        return accessrank;
    }

    public void setAccessrank(Integer accessrank) {
        this.accessrank = accessrank;
    }
}
