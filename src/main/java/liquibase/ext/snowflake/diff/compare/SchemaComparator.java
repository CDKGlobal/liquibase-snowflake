package liquibase.ext.snowflake.diff.compare;

/**
 * Created by vesterma on 10/09/2014.
 */

import liquibase.database.Database;
import liquibase.diff.ObjectDifferences;
import liquibase.diff.compare.CompareControl;
import liquibase.diff.compare.DatabaseObjectComparator;
import liquibase.diff.compare.DatabaseObjectComparatorChain;
import liquibase.ext.snowflake.database.SnowflakeDatabase;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Schema;

import java.util.Set;


public class SchemaComparator implements DatabaseObjectComparator {
    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof SnowflakeDatabase) {
            if (Schema.class.isAssignableFrom(objectType)) {
                return PRIORITY_DATABASE;
            }
        }
        return PRIORITY_NONE;
    }

    public SchemaComparator() {
        super();
    }

    @Override
    public String[] hash(DatabaseObject databaseObject, Database accordingTo, DatabaseObjectComparatorChain chain) {
        return chain.hash(databaseObject, accordingTo);
    }

    @Override
    public boolean isSameObject(DatabaseObject databaseObject1, DatabaseObject databaseObject2, Database accordingTo, DatabaseObjectComparatorChain chain) {
        if (!(databaseObject1 instanceof Schema && databaseObject2 instanceof Schema)) {
            return false;
        }

        String schema1 = ((Schema) databaseObject1).getName();
        String schema2 = ((Schema) databaseObject2).getName();

        if (schema1 == null) {
            return schema2 == null;
        }
        return schema1.equalsIgnoreCase(schema2);


    }


    @Override
    public ObjectDifferences findDifferences(DatabaseObject databaseObject1, DatabaseObject databaseObject2, Database accordingTo, CompareControl compareControl, DatabaseObjectComparatorChain chain, Set<String> exclude) {
        ObjectDifferences differences = new ObjectDifferences(compareControl);
        differences.compare("name", databaseObject1, databaseObject2, new ObjectDifferences.DatabaseObjectNameCompareFunction(Schema.class, accordingTo));

        return differences;
    }

}
