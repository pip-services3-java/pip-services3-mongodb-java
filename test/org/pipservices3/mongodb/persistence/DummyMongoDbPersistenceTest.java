package org.pipservices3.mongodb.persistence;

import org.junit.Test;
import org.pipservices3.commons.convert.*;
import org.pipservices3.commons.config.ConfigParams;
import org.pipservices3.commons.errors.ApplicationException;
import org.pipservices3.mongodb.fixtures.DummyPersistenceFixture;

public class DummyMongoDbPersistenceTest {

	private DummyMongoDbPersistence _persistence;
	private DummyPersistenceFixture _fixture;

	public DummyMongoDbPersistenceTest() throws ApplicationException {
        String mongoEnabled = System.getenv("MONGO_SERVICE_URI") != null ? System.getenv("MONGO_SERVICE_URI") : "true";
        String mongoUri = System.getenv("MONGO_URI");
        String mongoHost = System.getenv("MONGO_SERVICE_HOST") != null ? System.getenv("MONGO_SERVICE_HOST") : "localhost";
        String mongoPort = System.getenv("MONGO_SERVICE_PORT") != null ? System.getenv("MONGO_SERVICE_PORT") : "27017";
        String mongoDatabase = System.getenv("MONGO_DB") != null ? System.getenv("MONGO_DB") : "test";
        
        boolean enabled = BooleanConverter.toBoolean(mongoEnabled);
        if (enabled) {
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

            _fixture = new DummyPersistenceFixture(_persistence);
        }
    }
	
	@Test
	public void testCrudOperations() throws ApplicationException {
        if (_fixture != null)
		    _fixture.testCrudOperations();
    }
	
	@Test
	public void testBatchOperations() throws ApplicationException {
        if (_fixture != null)
		    _fixture.testBatchOperations();
    }
	
}
