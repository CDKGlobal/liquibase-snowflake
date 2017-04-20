package liquibase.ext.snowflake.structure;

import liquibase.structure.core.Relation;

/**
 * Created with IntelliJ IDEA.
 * User: vesterma
 * Date: 11/11/13
 * Time: 16:47
 * To change this template use File | Settings | File Templates.
 */
public class Projection extends Relation {


    public String getAnchorTable(){
        return getAttribute("anchorTable", String.class);
    }

    public void setAnchorTable(String anchorTable){
        setAttribute("anchorTable", anchorTable);
    }

    public String getDefinition() {
        return getAttribute("definition", String.class);
    }

    public void setDefinition(String definition) {
        this.setAttribute("definition", definition);
    }

    public String getSubquery() {
        return getAttribute("subquery", String.class);
    }

    public void setSubquery(String subquery) {
        this.setAttribute("subquery", subquery);
    }

    public String getKSafe() {
        return getAttribute("ksafe", String.class);
    }

    public void setKSafe(String kSafe) {
        this.setAttribute("ksafe", kSafe);
    }

    public String getOrderBy() {
        return getAttribute("orderby", String.class);
    }

    public void setOrderBy(String orderBy) {
        this.setAttribute("orderby", orderBy);
    }
    public void setSegmentation(Segmentation segmentation){
        this.setAttribute("segmentation", segmentation);
    }

    public Segmentation getSegmentation(){
        return getAttribute("segmentation", Segmentation.class);
    }

    public String getSegmentedBy() {
        return getAttribute("segmentedby", String.class);
    }

    public void setSegmentedBy(String segmentedBy) {
        this.setAttribute("segmentedby", segmentedBy);
    }


    public void setOffset(Long offset) {
        this.setAttribute("offset", offset);
    }
    public Long getOffset() {
        return this.getAttribute("offset", Long.class);
    }

    public void setIsSegmented(boolean isSegmented){
        this.setAttribute("isSegmented", isSegmented);
    }

    public boolean getIsSegmented(){
        if (this.getAttribute("isSegmented", boolean.class) != null)
            return this.getAttribute("isSegmented", boolean.class);
        return false;
    }

    public String getNodes(){
        return getAttribute("nodes", String.class);
    }

    public void setNodes(String nodes){
        setAttribute("nodes", nodes);
    }
}
