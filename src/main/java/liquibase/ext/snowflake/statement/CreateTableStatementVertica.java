package liquibase.ext.snowflake.statement;

import liquibase.datatype.LiquibaseDataType;
import liquibase.ext.snowflake.structure.Segmentation;
import liquibase.statement.ColumnConstraint;
import liquibase.statement.core.CreateTableStatement;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: vesterma
 * Date: 11/11/13
 * Time: 11:54
 * To change this template use File | Settings | File Templates.
 */
public class CreateTableStatementVertica extends CreateTableStatement{

    private Segmentation segmentation;
    private String orderby;
    private String subquery;
    private String partitionby;
    private Integer ksafe;


    private String projectionName;
    //private List<ColumnConfigVertica> columns = new ArrayList<ColumnConfigVertica>();
    private Map<String, String> columnEncodings = new HashMap<String, String>();
    private Map<String, Integer> columnAccessrank = new HashMap<String, Integer>();

    public CreateTableStatementVertica(String catalogName, String schemaName, String tableName) {
        super(catalogName, schemaName, tableName);
    }

    public String getColumnEncoding(String column) {
        return columnEncodings.get(column);
    }

    public Integer getColumnAccessrank(String column) {
        return columnAccessrank.get(column);
    }

    public Map<String, String> getColumnEncodings() {
        return columnEncodings;
    }

    public CreateTableStatement addPrimaryKeyColumn(String columnName, LiquibaseDataType columnType, Object defaultValue, String keyName, String encoding, Integer accessrank,String tablespace, ColumnConstraint... constraints){
        this.columnEncodings.put(columnName, encoding);
        this.columnAccessrank.put(columnName,accessrank);
        this.addPrimaryKeyColumn(columnName, columnType, defaultValue, keyName,tablespace,constraints);
        return this;
    }

    public CreateTableStatement addColumn(String columnName, LiquibaseDataType columnType, Object defaultValue, String encoding, String remarks, ColumnConstraint... constraints){
        this.columnEncodings.put(columnName, encoding);
        this.addColumn(columnName, columnType, defaultValue, remarks, constraints);
        return this;
    }


    public CreateTableStatement addColumn(String columnName, LiquibaseDataType columnType, Object defaultValue, String encoding, Integer accessrank, String remarks, ColumnConstraint... constraints){
        this.columnEncodings.put(columnName, encoding);
        this.columnAccessrank.put(columnName,accessrank);
        this.addColumn(columnName, columnType, defaultValue, constraints);
        return this;
    }

    public Segmentation getSegmentation() {
        return segmentation;
    }

    public void setSegmentation(Segmentation segmentation) {
        this.segmentation = segmentation;
    }

    public String getOrderby() {
        return orderby;
    }

    public void setOrderby(String orderby) {
        this.orderby = orderby;
    }

    public String getSubquery() {
        return subquery;
    }

    public void setSubquery(String subquery) {
        this.subquery = subquery;
    }

    public Integer getKsafe() {
        return ksafe;
    }

    public void setKsafe(Integer ksafe) {
        this.ksafe = ksafe;
    }

    public String getPartitionby() {
        return partitionby;
    }

    public void setPartitionby(String partitionby) {
        this.partitionby = partitionby;
    }
}
