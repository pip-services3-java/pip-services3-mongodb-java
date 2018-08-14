package org.pipservices.mongodb;

import org.junit.Test;
import org.pipservices.commons.config.ConfigParams;
import org.pipservices.commons.errors.ApplicationException;
import org.pipservices.fixtures.*;

public class IdentifiableMongoDbPersistenceTest {

	private static MongoDbDummyPersistence Db = new MongoDbDummyPersistence();
	public static MongoDbDummyPersistence getDb() { return Db; }
	
	private static PersistenceFixture Fixture;
    public static PersistenceFixture getFixture() { return Fixture; }
	public static void setFixture(PersistenceFixture fixture) { Fixture = fixture; }

	public IdentifiableMongoDbPersistenceTest() throws ApplicationException {
        String mongoUri = System.getenv("MONGO_URI") != null ? System.getenv("MONGO_URI") : "mongodb://localhost:27017/test";
        String mongoHost = System.getenv("MONGO_HOST") != null ? System.getenv("MONGO_HOST") : "localhost";
        String mongoPort = System.getenv("MONGO_PORT") != null ? System.getenv("MONGO_PORT") : "27017";
        String mongoDatabase = System.getenv("MONGO_DB") != null ? System.getenv("MONGO_DB") : "test";

        if (mongoUri == null && mongoHost == null)
            return;

        if (Db == null) return;

        Db.configure(ConfigParams.fromTuples(
            "connection.uri", mongoUri,
            "connection.host", mongoHost,
            "connection.port", mongoPort,
            "connection.database", mongoDatabase
        ));

        Db.open(null);
        Db.clear(null);

        Fixture = new PersistenceFixture(Db);
    }
	
	@Test
	public void testCrudOperations() {
        Fixture.testCrudOperations();
    }
	
	@Test
	public void testGetById() {
        Fixture.testGetById();
    }
	
	@Test
	public void testGetByIdFromArray() {
        Fixture.testGetByIdFromArray();
    }
	
	@Test
	public void testGetPageByFilter() {
        Fixture.testGetPageByFilter();
    }
	
	@Test
	public void testModifyExistingPropertiesBySelectedFields() {
        Fixture.testModifyExistingPropertiesBySelectedFields();
    }
	
	@Test
	public void testModifyNullPropertiesBySelectedFields() {
        Fixture.testModifyNullPropertiesBySelectedFields();
    }
	
	@Test
	public void testModifyNestedCollection() {
        Fixture.testModifyNestedCollection();
    }
	
	@Test
	public void testSearchWithinNestedCollectionByFilter() {
        Fixture.testSearchWithinNestedCollectionByFilter();
    }
	
	@Test
	public void testSearchWithinDeepNestedCollectionByFilter() {
        Fixture.testSearchWithinDeepNestedCollectionByFilter();
    }
	
	@Test
	public void testGetPageByIdsFilter() {
        Fixture.testGetPageByIdsFilter();
    }
	
	@Test
	public void testGetPageSortedByOneField() {
        Fixture.testGetPageSortedByOneField();
    }
}
