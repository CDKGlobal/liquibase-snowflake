package liquibase.ext.snowflake.structure;

/**
 * Created with IntelliJ IDEA.
 * User: vesterma
 * Date: 01/12/13
 * Time: 16:52
 * To change this template use File | Settings | File Templates.
 */
public class Segmentation {
    private String expression;
    private String nodes;
    private Boolean allNodes;
    private Integer offset;
    private Boolean unsegmented = false;

    public Segmentation() { }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public String getNodes() {
        return nodes;
    }

    public void setNodes(String nodes) {
        this.nodes = nodes;
    }

    public Boolean getAllNodes() {
        return allNodes;
    }

    public void setAllNodes(Boolean allNodes) {
        this.allNodes = allNodes;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public Boolean getUnsegmented() { return unsegmented;   }

    public void setUnsegmented(Boolean unsegmented) { this.unsegmented = unsegmented;   }
}