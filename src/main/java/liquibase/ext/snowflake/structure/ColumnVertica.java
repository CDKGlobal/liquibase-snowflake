package liquibase.ext.snowflake.structure;

import liquibase.structure.core.Column;

/**
 * Created with IntelliJ IDEA.
 * User: vesterma
 * Date: 11/11/13
 * Time: 16:47
 * To change this template use File | Settings | File Templates.
 */
public class ColumnVertica extends Column {

    public ColumnVertica() {
    }

    public ColumnVertica setEncoding(String encoding) {
        setAttribute("encoding", encoding);
        return this;
    }

    public String getEncoding() {
        return getAttribute("encoding", String.class);
    }


}
