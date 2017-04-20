package liquibase.ext.snowflake.snapshot;

import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.ext.snowflake.database.SnowflakeDatabase;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.jvm.IndexSnapshotGenerator;
import liquibase.structure.DatabaseObject;

/**
 * Created with IntelliJ IDEA.
 * User: vesterma
 * Date: 15/11/13
 * Time: 23:13
 * To change this template use File | Settings | File Templates.
 */
public class IndexSnapshotGeneratorVertica extends IndexSnapshotGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof SnowflakeDatabase)
            return PRIORITY_DATABASE;
        return PRIORITY_NONE;

    }

    @Override
    protected void addTo(DatabaseObject foundObject, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        System.out.println("in index addTo");
        return null;
    }

    @Override
    public Class<? extends SnapshotGenerator>[] replaces(){
        return new Class[]{IndexSnapshotGenerator.class};
    }

}
