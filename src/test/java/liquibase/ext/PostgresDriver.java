package liquibase.ext;

import liquibase.Liquibase;
import liquibase.database.DatabaseConnection;
import liquibase.database.DatabaseFactory;
import liquibase.database.core.PostgresDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.ext.database.SnowflakeDatabase;
import liquibase.resource.FileSystemResourceAccessor;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class PostgresDriver {
    public static void main(String[] args) throws Exception {

        PostgresDatabase database = new PostgresDatabase();
        database.setDefaultSchemaName("szalwinb_wca5");

        DatabaseFactory.getInstance().clearRegistry();
        DatabaseFactory.getInstance().register(database);
        Properties myProp = new Properties();

        myProp.put("user", "szalwinb_wca5");
        myProp.put("password", "szalwinb_wca5");

        String url1 = "jdbc:postgresql://ordndmgpmdw00.dslab.ad.adp.com:5432/fmd1";

        Driver driver = (Driver) Class.forName(DatabaseFactory.getInstance().findDefaultDriver(url1), true,
                Thread.currentThread().getContextClassLoader()).newInstance();
        Connection connection = driver.connect(url1, myProp);
        database.setConnection(new JdbcConnection(connection));

        Liquibase liquibase = null;

        try {
            URL url = PostgresDriver.class.getClassLoader().getResource("snowflake.xml");
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
