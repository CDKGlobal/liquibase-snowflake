package liquibase.ext.snowflake.structure;

import liquibase.ext.snowflake.change.ColumnConfigVertica;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: vesterma
 * Date: 02/12/13
 * Time: 14:31
 * To change this template use File | Settings | File Templates.
 */
public class GroupedColumns {

    private List<ColumnConfigVertica> columns;

    public GroupedColumns() {
        columns = new ArrayList<ColumnConfigVertica>(2);
    }

    public ColumnConfigVertica createColumnv(){
        ColumnConfigVertica col = new ColumnConfigVertica();
        columns.add(col);
        return col;
    }

    public List<ColumnConfigVertica> getColumns() {
        return columns;
    }
}
