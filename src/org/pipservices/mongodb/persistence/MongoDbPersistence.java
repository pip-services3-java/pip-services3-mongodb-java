package org.pipservices.mongodb.persistence;

import org.bson.codecs.configuration.*;
import org.bson.codecs.pojo.*;
import org.pipservices.commons.config.*;
import org.pipservices.commons.errors.*;
import org.pipservices.components.log.*;
import org.pipservices.mongodb.connect.*;
import org.pipservices.commons.refer.*;
import org.pipservices.commons.run.*;

import com.mongodb.*;
import com.mongodb.client.*;


public class MongoDbPersistence<T> implements IReferenceable, IReconfigurable, IOpenable, ICleanable {

	
	private ConfigParams _defaultConfig = ConfigParams.fromTuples(
//        "connection.type", "mongodb",
//        "connection.database", "test",
//        "connection.host", "localhost",
//        "connection.port", 27017,
//
//        "options.poll_size", 4,
//        "options.keep_alive", 1,
//        "options.connect_timeout", 5000,
//        "options.auto_reconnect", true,
//        "options.max_page_size", 100,
//        "options.debug", true
    );
	
	protected String _collectionName;
    protected MongoDbConnectionResolver _connectionResolver = new MongoDbConnectionResolver();
    protected ConfigParams _options = new ConfigParams();
    protected Object _lock = new Object();

    protected MongoClient _connection;
    protected MongoDatabase _database;
    protected MongoCollection<T> _collection;
    protected Class<T> _documentClass;
    
    protected CompositeLogger _logger = new CompositeLogger();

    public MongoDbPersistence(String collectionName, Class<T> documentClass) {
        if (collectionName == null)
            throw new NullPointerException(collectionName);

        _collectionName = collectionName;
        _documentClass = documentClass;
    }
	
    public void setReferences(IReferences references) {
        _logger.setReferences(references);
        _connectionResolver.setReferences(references);
    }

    public void configure(ConfigParams config) {
        config = config.setDefaults(_defaultConfig);

        _connectionResolver.configure(config);

        _collectionName = config.getAsStringWithDefault("collection", _collectionName);

        _options = _options.override(config.getSection("options"));
    }

    
	@Override
	public boolean isOpen() {
		return _collection != null;
	}
   
    @SuppressWarnings("unchecked")
	public void open(String correlationId) throws ApplicationException {
        String uri = _connectionResolver.resolve(correlationId);

        _logger.trace(correlationId, "Connecting to mongodb");
        
        try {
        	MongoClientURI clientUri = new MongoClientURI(uri);
        	String databaseName = clientUri.getDatabase();
        	
        	_connection = new MongoClient(clientUri);

            PojoCodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();
            //JacksonCodecProvider dateCodecProvider = new JacksonCodecProvider(ObjectMapperFactory.createObjectMapper());            
            CodecRegistry pojoCodecRegistry = CodecRegistries.fromRegistries(
        		MongoClient.getDefaultCodecRegistry(),
        		CodecRegistries.fromProviders(pojoCodecProvider)
        		//CodecRegistries.fromProviders(dateCodecProvider)
    		);
        	_database = _connection.getDatabase(databaseName).withCodecRegistry(pojoCodecRegistry);

            _collection =  (MongoCollection<T>)_database.getCollection(_collectionName, _documentClass);
//            if (_collection == null)
//            	_database.createCollection(_collectionName);
//            _collection =  (MongoCollection<T>)_database.getCollection(_collectionName, _documentClass);

            _logger.debug(correlationId, "Connected to mongodb database %s, collection %s", databaseName, _collectionName);
        } catch (Exception ex) {
        	_connection = null;
        	
            throw new ConnectionException(correlationId, "Connection to mongodb failed", ex.toString());
        }
    }
    
    
	@Override
	public void close(String correlationId) {
		if (_connection != null) {
			_connection.close();
			
			_connection = null;
			_database = null;
			_collection = null;
		}
	}

	@Override
	public void clear(String correlationId) {
		_collection.drop();
		
	}

}
