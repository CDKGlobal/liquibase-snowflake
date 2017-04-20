package liquibase.ext.snowflake.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.snowflake.change.ColumnConfigVertica;
import liquibase.ext.snowflake.database.SnowflakeDatabase;
import liquibase.ext.snowflake.statement.CreateProjectionStatement;
import liquibase.ext.snowflake.structure.GroupedColumns;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

/**
 * Created with IntelliJ IDEA.
 * User: vesterma
 * Date: 11/11/13
 * Time: 11:15
 * To change this template use File | Settings | File Templates.
 */
public class CreateProjectionGeneratorVertica extends AbstractSqlGenerator<CreateProjectionStatement> {
    @Override
    public ValidationErrors validate(CreateProjectionStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();

        validationErrors.checkRequiredField("projectionName", statement.getProjectionName());
        //TODO: check if validation not needed.
		//validationErrors.checkRequiredField("subquery", statement.getSubquery());
        return validationErrors;

        /*validationErrors.checkRequiredField("tableName", statement.getTableName());
        validationErrors.checkRequiredField("columns", statement.getColumns());

        for (ColumnConfig column : statement.getColumns()) {
            if (column.getConstraints() != null && column.getConstraints().isPrimaryKey() && (database instanceof CacheDatabase
                    || database instanceof H2Database
                    || database instanceof DB2Database
                    || database instanceof DerbyDatabase
                    || database instanceof SQLiteDatabase)) {
                validationErrors.addError("Adding primary key columns is not supported on "+database.getShortName());
            }
        }*/
    }

    @Override
    public Sql[] generateSql(CreateProjectionStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        StringBuilder sql = new StringBuilder();

        sql.append( "create PROJECTION ");
        if (statement.getSchemaName() != null)
            sql.append(statement.getSchemaName()).append(".");
        else
            sql.append(database.getDefaultSchemaName()).append(".");
        if (statement.getProjectionName() != null) {
            sql.append(statement.getProjectionName())
                    .append(" ");
        }

        if (!statement.getColumns().isEmpty() || !statement.getGroupedColumns().isEmpty()){
            sql.append("(");
            String delim = "";
            for (ColumnConfigVertica column : statement.getColumns()) {
                sql.append(delim).append(" ").append(column.getName());
                if(column.getEncoding() != null) sql.append(" ENCODING ").append(column.getEncoding());
                if(column.getAccessrank() != null)sql.append(" ").append(column.getAccessrank());
                delim = ",";
            }
            if (!statement.getGroupedColumns().isEmpty()){

                for (GroupedColumns group : statement.getGroupedColumns()) {
                    sql.append(delim).append(" GROUPED (");
                    delim = "";
                    for (ColumnConfigVertica column : group.getColumns()) {
                        sql.append(delim).append(" ").append(column.getName());
                        if(column.getEncoding() != null) sql.append(" ENCODING ").append(column.getEncoding());
                        if(column.getAccessrank() != null)sql.append(" ").append(column.getAccessrank());
                        delim = ",";
                    }
                    sql.append(")");
                }

            }
            sql.append(")");
        }

        sql.append(" AS ");
        if (statement.getSubquery() != null)
            sql.append(statement.getSubquery());

        if(statement.getOrderby() != null)
            sql.append(" ORDER BY ").append(statement.getOrderby());



            if (statement.getSegmentedby()==null){
                sql.append(" UNSEGMENTED ");
            }else{
                sql.append(" SEGMENTED BY ");

                sql.append(statement.getSegmentedby());

            }
            if (statement.getNodes().contains("ALL") ){
                sql.append(" ALL NODES ");
                if (statement.getOffset() != null)
                    sql.append(" OFFSET ").append(statement.getOffset().toString());
            }else{
                sql.append(" ").append(statement.getNodes());
            }




        if (statement.getKsafe() != null){
            sql.append(" KSAFE ");
            if (!statement.getKsafe().isEmpty())
                sql.append(statement.getKsafe());
        }

       /* for (ColumnConfigVertica column : statement.getColumns()) {

        }*/
  /*      for (ColumnConfig column : statement.getColumns()) {
            String alterTable = "ALTER TABLE " + database.escapeTableName(null, statement.getSchemaName(), statement.getTableName());

            // add "MODIFY"
            alterTable += " " + getModifyString(database) + " ";

            // add column name
            alterTable += database.escapeColumnName(null, statement.getSchemaName(), statement.getTableName(), column.getName());

            alterTable += getPreDataTypeString(database); // adds a space if nothing else

            // add column type
            alterTable += DataTypeFactory.getInstance().fromDescription(column.getType()).toDatabaseDataType(database);

            if (supportsExtraMetaData(database)) {
                if (column.getConstraints() != null && !column.getConstraints().isNullable()) {
                    alterTable += " NOT NULL";
                } else {
                    if (database instanceof SybaseDatabase || database instanceof SybaseASADatabase) {
                        alterTable += " NULL";
                    }
                }

                alterTable += getDefaultClause(column, database);

                if (column.isAutoIncrement() != null && column.isAutoIncrement()) {
                    alterTable += " " + database.getAutoIncrementClause(BigInteger.ONE, BigInteger.ONE);
                }

                if (column.getConstraints() != null && column.getConstraints().isPrimaryKey()) {
                    alterTable += " PRIMARY KEY";
                }
            }

            alterTable += getPostDataTypeString(database);

            sql.add(new UnparsedSql(alterTable));
        }*/
        System.out.println(sql.toString());
        return new Sql[]
                {
                        new UnparsedSql(sql.toString())
                };

//        return sql.toArray(new Sql[sql.size()]);

    }

    @Override
    public boolean supports(CreateProjectionStatement statement, Database database) {
        return database instanceof SnowflakeDatabase;
    }
}
