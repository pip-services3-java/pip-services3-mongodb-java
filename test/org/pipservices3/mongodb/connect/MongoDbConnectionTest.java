package org.pipservices3.mongodb.connect;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.pipservices3.commons.config.ConfigParams;
import org.pipservices3.commons.errors.ApplicationException;

public class MongoDbConnectionTest {
    MongoDbConnection connection;

    String mongoUri = System.getenv("MONGO_SERVICE_URI");
    String mongoHost = System.getenv("MONGO_SERVICE_HOST") != null ? System.getenv("MONGO_SERVICE_HOST") : "localhost";
    int mongoPort = System.getenv("MONGO_SERVICE_PORT") != null ? Integer.valueOf(System.getenv("MONGO_SERVICE_PORT")) : 27017;
    String mongoDatabase = System.getenv("MONGO_DB") != null ? System.getenv("MONGO_DB") : "test";

    boolean enabled = false;

    public MongoDbConnectionTest() {

        // Skip tests
        if (mongoUri != null || mongoHost != null)
            enabled = true;
    }

    @Before
    public void setup() throws ApplicationException {
        var dbConfig = ConfigParams.fromTuples(
                "connection.uri", mongoUri,
                "connection.host", mongoHost,
                "connection.port", mongoPort,
                "connection.database", mongoDatabase
        );

        connection = new MongoDbConnection();
        connection.configure(dbConfig);

        connection.open(null);
    }

    @After
    public void teardown() throws ApplicationException {
        connection.close(null);
    }


    @Test
    public void testOpenAndClose() {
        if (enabled) {
            assertNotNull(connection.getConnection());
            assertNotNull(connection.getDatabase());
            assertNotNull(connection.getDatabaseName());
        }
    }

}
