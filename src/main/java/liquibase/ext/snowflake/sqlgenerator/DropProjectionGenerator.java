package liquibase.ext.snowflake.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.snowflake.database.SnowflakeDatabase;
import liquibase.ext.snowflake.statement.DropProjectionStatement;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.structure.core.Relation;
import liquibase.structure.core.Table;

/**
 * Created with IntelliJ IDEA.
 * User: vesterma
 * Date: 11/11/13
 * Time: 14:24
 * To change this template use File | Settings | File Templates.
 */
public class DropProjectionGenerator extends AbstractSqlGenerator<DropProjectionStatement> {
    @Override
    public ValidationErrors validate(DropProjectionStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("projectionName", statement.getProjectionName());

        return validationErrors;
    }

    @Override
    public Sql[] generateSql(DropProjectionStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {

        StringBuffer buffer = new StringBuffer();

        buffer.append("DROP PROJECTION ");
        if (statement.getSchemaName() != null)
            buffer.append(statement.getSchemaName()).append(".");
        if (statement.getProjectionName() != null) {
            buffer.append(statement.getProjectionName())
                    .append(" ");
        }
        if (statement.isCascade()) {
            buffer.append(" CASCADE");

        }

        return new Sql[]{
                new UnparsedSql(buffer.toString(), getAffectedTable(statement))
        };
    }

    protected Relation getAffectedTable(DropProjectionStatement statement) {
        return new Table().setName(statement.getTableName()).setSchema("", statement.getSchemaName());
    }

    @Override
    public boolean supports(DropProjectionStatement statement, Database database) {
        return database instanceof SnowflakeDatabase;
    }

}
