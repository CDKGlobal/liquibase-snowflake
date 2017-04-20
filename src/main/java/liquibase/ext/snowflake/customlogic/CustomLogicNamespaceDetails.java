package liquibase.ext.snowflake.customlogic;

import liquibase.parser.LiquibaseParser;
import liquibase.parser.NamespaceDetails;
import liquibase.parser.core.xml.XMLChangeLogSAXParser;
import liquibase.serializer.LiquibaseSerializer;
import liquibase.serializer.core.xml.XMLChangeLogSerializer;

/**
 * Created by vesterma on 03/03/14.
 */

public class CustomLogicNamespaceDetails implements NamespaceDetails {

    public static final String CUSTOM_LOGIC_NAMESPACE = "http://www.liquibase.org/xml/ns/dbchangelog-ext";
    public static final String CUSTOM_LOGIC_XSD = "../liquibase/ext/vertica/xml/dbchangelog-ext.xsd";

    @Override
    public int getPriority() {
        return PRIORITY_EXTENSION;
    }

    @Override
    public boolean supports(LiquibaseSerializer serializer, String namespace) {
        if (namespaceCorrect(namespace) && serializer instanceof XMLChangeLogSerializer) {
            return true;
        }
        return false;
    }

    @Override
    public boolean supports(LiquibaseParser parser, String namespace) {
        if (namespaceCorrect(namespace) && parser instanceof XMLChangeLogSAXParser) {
            return true;
        }
        return false;
    }

    private boolean namespaceCorrect(String namespace) {
        return namespace.equals(CUSTOM_LOGIC_NAMESPACE) || namespace.equals(CUSTOM_LOGIC_XSD);
    }

    @Override
    public String getShortName(String namespace) {
        return "vert";
    }

    @Override
    public String getSchemaUrl(String namespace) {
        return CUSTOM_LOGIC_XSD;
    }

    @Override
    public String[] getNamespaces() {
//        return new String[0];
        String[] namespaces = {"vert"};
        return namespaces;
    }


    @Override
    public String getLocalPath(String namespace) {
        return "xml/dbchangelog-ext.xsd";

    }

}
