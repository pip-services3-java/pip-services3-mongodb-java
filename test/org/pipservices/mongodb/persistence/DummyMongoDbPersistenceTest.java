package org.pipservices.mongodb.persistence;

import org.junit.Test;
import org.pipservices.commons.config.ConfigParams;
import org.pipservices.commons.errors.ApplicationException;

public class DummyMongoDbPersistenceTest {

	private DummyMongoDbPersistence _persistence;	
	private PersistenceFixture _fixture;

	public DummyMongoDbPersistenceTest() throws ApplicationException {
        String mongoUri = System.getenv("MONGO_URI");
        String mongoHost = System.getenv("MONGO_HOST") != null ? System.getenv("MONGO_HOST") : "localhost";
        String mongoPort = System.getenv("MONGO_PORT") != null ? System.getenv("MONGO_PORT") : "27017";
        String mongoDatabase = System.getenv("MONGO_DB") != null ? System.getenv("MONGO_DB") : "test";

        if (mongoUri == null && mongoHost == null)
            return;

        _persistence = new DummyMongoDbPersistence();
        		
        _persistence.configure(ConfigParams.fromTuples(
            "connection.uri", mongoUri,
            "connection.host", mongoHost,
            "connection.port", mongoPort,
            "connection.database", mongoDatabase
        ));

        _persistence.open(null);
        _persistence.clear(null);

        _fixture = new PersistenceFixture(_persistence);
    }
	
	@Test
	public void testCrudOperations() throws ApplicationException {
		_fixture.testCrudOperations();
    }
	
	@Test
	public void testBatchOperations() throws ApplicationException {
		_fixture.testBatchOperations();
    }
	
}
