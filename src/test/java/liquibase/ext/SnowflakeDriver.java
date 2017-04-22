package liquibase.ext;

import liquibase.Liquibase;
import liquibase.database.DatabaseConnection;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.ext.database.SnowflakeDatabase;
import liquibase.resource.FileSystemResourceAccessor;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class SnowflakeDriver {
    public static void main(String[] args){

        SnowflakeDatabase database = new SnowflakeDatabase();
        database.setDefaultSchemaName("bruces");

        DatabaseFactory.getInstance().clearRegistry();
        DatabaseFactory.getInstance().register(database);
        Properties myProp = new Properties();

        myProp.put("user", "BRUCES");
        myProp.put("password", "q2iSyyjGtTTfAYD0y50N");
        myProp.put("db", "BRUCE_TEST");
        myProp.put("warehouse", "bruce_wh");
        myProp.put("schema", "BRUCES");
        Connection conn = null;
        try {
            conn = DriverManager.getConnection( "jdbc:snowflake://ti87565.snowflakecomputing.com/", myProp);
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        DatabaseConnection dc = new JdbcConnection(conn);
        database.setConnection(dc);

        Liquibase liquibase = null;

        try {
            URL url = SnowflakeDriver.class.getClassLoader().getResource("snowflake.xml");
            // this code translates the project relative path to an absolute one.
            File f;
            try {
                f = new File(url.toURI());
            } catch(URISyntaxException e) {
                f = new File(url.getPath());
            }
            String changelog = f.getAbsolutePath();

            liquibase = new Liquibase(changelog,new FileSystemResourceAccessor(), database);
            liquibase.update(11,"");
        } catch (LiquibaseException e) {
            e.printStackTrace();
        }
    }
}
