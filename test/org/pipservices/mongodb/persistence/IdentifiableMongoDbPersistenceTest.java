package org.pipservices.mongodb.persistence;

import org.junit.Test;
import org.pipservices.commons.config.ConfigParams;
import org.pipservices.commons.errors.ApplicationException;

public class IdentifiableMongoDbPersistenceTest {

	private MongoDbDummyPersistence _persistence;	
	private PersistenceFixture _fixture;

	public IdentifiableMongoDbPersistenceTest() throws ApplicationException {
        String mongoUri = System.getenv("MONGO_URI");
        String mongoHost = System.getenv("MONGO_HOST") != null ? System.getenv("MONGO_HOST") : "localhost";
        String mongoPort = System.getenv("MONGO_PORT") != null ? System.getenv("MONGO_PORT") : "27017";
        String mongoDatabase = System.getenv("MONGO_DB") != null ? System.getenv("MONGO_DB") : "test";

        if (mongoUri == null && mongoHost == null)
            return;

        _persistence = new MongoDbDummyPersistence();
        		
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
	public void testCrudOperations() {
		if (_fixture != null)
			_fixture.testCrudOperations();
    }
	
	@Test
	public void testGetById() {
		if (_fixture != null)
			_fixture.testGetById();
    }
	
	@Test
	public void testGetByIdFromArray() {
		if (_fixture != null)
			_fixture.testGetByIdFromArray();
    }
	
//	@Test
//	public void testGetPageByFilter() {
//		if (_fixture != null)
//			_fixture.testGetPageByFilter();
//    }
//	
//	@Test
//	public void testModifyExistingPropertiesBySelectedFields() {
//		if (_fixture != null)
//			_fixture.testModifyExistingPropertiesBySelectedFields();
//    }
//	
//	@Test
//	public void testModifyNullPropertiesBySelectedFields() {
//		if (_fixture != null)
//			_fixture.testModifyNullPropertiesBySelectedFields();
//    }
//	
//	@Test
//	public void testModifyNestedCollection() {
//		if (_fixture != null)
//			_fixture.testModifyNestedCollection();
//    }
//	
//	@Test
//	public void testSearchWithinNestedCollectionByFilter() {
//		if (_fixture != null)
//			_fixture.testSearchWithinNestedCollectionByFilter();
//    }
//	
//	@Test
//	public void testSearchWithinDeepNestedCollectionByFilter() {
//		if (_fixture != null)
//			_fixture.testSearchWithinDeepNestedCollectionByFilter();
//    }
//	
//	@Test
//	public void testGetPageByIdsFilter() {
//		if (_fixture != null)
//			_fixture.testGetPageByIdsFilter();
//    }
//	
//	@Test
//	public void testGetPageSortedByOneField() {
//		if (_fixture != null)
//			_fixture.testGetPageSortedByOneField();
//    }
}
