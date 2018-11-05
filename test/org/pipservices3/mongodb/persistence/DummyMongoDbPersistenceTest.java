package org.pipservices3.mongodb.persistence;

import org.junit.Test;
import org.pipservices3.commons.convert.*;
import org.pipservices3.commons.config.ConfigParams;
import org.pipservices3.commons.errors.ApplicationException;

public class DummyMongoDbPersistenceTest {

	private DummyMongoDbPersistence _persistence;	
	private PersistenceFixture _fixture;

	public DummyMongoDbPersistenceTest() throws ApplicationException {
        String mongoEnabled = System.getenv("MONGO_ENABLED") != null ? System.getenv("MONGO_ENABLED") : "true";
        String mongoUri = System.getenv("MONGO_URI");
        String mongoHost = System.getenv("MONGO_HOST") != null ? System.getenv("MONGO_HOST") : "localhost";
        String mongoPort = System.getenv("MONGO_PORT") != null ? System.getenv("MONGO_PORT") : "27017";
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

            _fixture = new PersistenceFixture(_persistence);
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
