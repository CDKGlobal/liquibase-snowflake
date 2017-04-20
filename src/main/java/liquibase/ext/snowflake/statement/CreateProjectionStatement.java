package liquibase.ext.snowflake.statement;

import liquibase.datatype.LiquibaseDataType;
import liquibase.ext.snowflake.change.ColumnConfigVertica;
import liquibase.ext.snowflake.structure.GroupedColumns;
import liquibase.ext.snowflake.structure.Segmentation;
import liquibase.statement.AbstractSqlStatement;
import liquibase.statement.AutoIncrementConstraint;
import liquibase.statement.ColumnConstraint;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: vesterma
 * Date: 11/11/13
 * Time: 11:22
 * To change this template use File | Settings | File Templates.
 */
public class CreateProjectionStatement extends AbstractSqlStatement {

    private String schemaName;
    private String tableName;
    private Segmentation segmentation;
    private String segmentedby;
    private String nodes;
    private Integer offset;
    private String orderby;
    private String subquery;
    private String ksafe;


    private String projectionName;
    private List<ColumnConfigVertica> columns = new ArrayList<ColumnConfigVertica>();
    private List<GroupedColumns> groupedColumns = new ArrayList<GroupedColumns>();
    private Set<AutoIncrementConstraint> autoIncrementConstraints = new HashSet<AutoIncrementConstraint>();



    public CreateProjectionStatement( String schemaName, String projectionName, String tableName, List<ColumnConfigVertica> columns) {

        this.schemaName = schemaName;
        this.tableName = tableName;
        this.projectionName = projectionName;
        this.columns = columns;
    }


    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getProjectionName() {
        return projectionName;
    }

    public List<ColumnConfigVertica> getColumns() {
        return columns;
    }

    public String getOrderby() {
        return orderby;
    }

    public void setOrderby(String orderby) {
        this.orderby = orderby;
    }

    public Segmentation getSegmentation() {
        return segmentation;
    }

    public void setSegmentation(Segmentation segmentation) {
        this.segmentation = segmentation;
    }

    public CreateProjectionStatement addColumn(String columnName, LiquibaseDataType columnType) {
        return addColumn(columnName, columnType, null, new ColumnConstraint[0]);
    }

    public CreateProjectionStatement addColumn(String columnName, LiquibaseDataType columnType, Object defaultValue,String encoding) {
        if (defaultValue instanceof ColumnConstraint) {
            return addColumn(columnName,  columnType, null, (ColumnConstraint) defaultValue);
        }
        return addColumn(columnName, columnType, defaultValue,encoding, new ColumnConstraint[0]);
    }

    public CreateProjectionStatement addColumn(String columnName, LiquibaseDataType columnType,String encoding, ColumnConstraint... constraints) {
        return addColumn(columnName, columnType, null, constraints);
    }

    public String getKsafe() {
        return ksafe;
    }

    public void setKsafe(String ksafe) {
        this.ksafe = ksafe;
    }

    public CreateProjectionStatement addColumn(String columnName, LiquibaseDataType columnType, Object defaultValue,String encoding, ColumnConstraint... constraints) {
//        this.getColumns().add(columnName);
        /*this.columnTypes.put(columnName, columnType);
        if (defaultValue != null) {
            defaultValues.put(columnName, defaultValue);
        }*/
        if (constraints != null) {
            for (ColumnConstraint constraint : constraints) {
                if (constraint == null) {
                    continue;
                }

                /*if (constraint instanceof PrimaryKeyConstraint) {
                    if (this.getPrimaryKeyConstraint() == null) {
                        this.primaryKeyConstraint = (PrimaryKeyConstraint) constraint;
                    } else {
                        for (String column : ((PrimaryKeyConstraint) constraint).getColumns()) {
                            this.getPrimaryKeyConstraint().addColumns(column);
                        }
                    }
                } else if (constraint instanceof NotNullConstraint) {
                    ((NotNullConstraint) constraint).setColumnName(columnName);
                    getNotNullColumns().add(columnName);
                } else if (constraint instanceof ForeignKeyConstraint) {
                    ((ForeignKeyConstraint) constraint).setColumn(columnName);
                    getForeignKeyConstraints().add(((ForeignKeyConstraint) constraint));
                } else if (constraint instanceof UniqueConstraint) {
                    ((UniqueConstraint) constraint).addColumns(columnName);
                    getUniqueConstraints().add(((UniqueConstraint) constraint));
                } else if (constraint instanceof AutoIncrementConstraint) {
                    autoIncrementConstraints.add((AutoIncrementConstraint) constraint);
                } else {
                    throw new RuntimeException("Unknown constraint type: " + constraint.getClass().getName());
                }*/
            }
        }

        return this;
    }



    public CreateProjectionStatement addColumnConstraint(AutoIncrementConstraint autoIncrementConstraint) {
        getAutoIncrementConstraints().add(autoIncrementConstraint);
        return this;
    }

    public Set<AutoIncrementConstraint> getAutoIncrementConstraints() {
        return autoIncrementConstraints;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getSubquery() {
        return subquery;
    }

    public void setSubquery(String subquery) {
        this.subquery = subquery;
    }

    public List<GroupedColumns> getGroupedColumns() {
        return groupedColumns;
    }

    public void setGroupedColumns(List<GroupedColumns> groupedColumns) {
        this.groupedColumns = groupedColumns;
    }

    public String getSegmentedby() {
        return segmentedby;
    }

    public void setSegmentedby(String segmentedby) {
        this.segmentedby = segmentedby;
    }

    public String getNodes() {
        return nodes;
    }

    public void setNodes(String nodes) {
        this.nodes = nodes;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }
}
