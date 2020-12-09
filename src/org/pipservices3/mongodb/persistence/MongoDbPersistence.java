package org.pipservices3.mongodb.persistence;

import org.bson.codecs.configuration.*;
import org.bson.codecs.pojo.*;
import org.pipservices3.commons.config.*;
import org.pipservices3.commons.errors.*;
import org.pipservices3.components.log.*;
import org.pipservices3.mongodb.codecs.*;
import org.pipservices3.mongodb.connect.*;
import org.pipservices3.commons.refer.*;
import org.pipservices3.commons.run.*;

import com.mongodb.*;
import com.mongodb.client.*;

/**
 * Abstract persistence component that stores data in MongoDB.
 * <p>
 * This is the most basic persistence component that is only
 * able to store data items of any type. Specific CRUD operations
 * over the data items must be implemented in child classes by
 * accessing <code>this._collection</code> or <code>this._model</code> properties.
 * <p>
 * ### Configuration parameters ###
 * <ul>
 * <li>collection:                  (optional) MongoDB collection name
 * <li>connection(s):    
 *   <ul>
 *   <li>discovery_key:             (optional) a key to retrieve the connection from <a href="https://pip-services3-java.github.io/pip-services3-components-java/org/pipservices3/components/connect/IDiscovery.html">IDiscovery</a>
 *   <li>host:                      host name or IP address
 *   <li>port:                      port number (default: 27017)
 *   <li>uri:                       resource URI or connection string with all parameters in it
 *   </ul>
 * <li>credential(s):    
 *   <ul>
 *   <li>store_key:                 (optional) a key to retrieve the credentials from <a href="https://pip-services3-java.github.io/pip-services3-components-java/org/pipservices3/components/auth/ICredentialStore.html">ICredentialStore</a>
 *   <li>username:                  (optional) user name
 *   <li>password:                  (optional) user password
 *   </ul>
 * <li>options:
 *   <ul>
 *   <li>max_pool_size:             (optional) maximum connection pool size (default: 2)
 *   <li>keep_alive:                (optional) enable connection keep alive (default: true)
 *   <li>connect_timeout:           (optional) connection timeout in milliseconds (default: 5 sec)
 *   <li>auto_reconnect:            (optional) enable auto reconnection (default: true)
 *   <li>max_page_size:             (optional) maximum page size (default: 100)
 *   <li>debug:                     (optional) enable debug output (default: false).
 *   </ul>
 * </ul>
 * <p>
 * ### References ###
 * <ul>
 * <li>*:logger:*:*:1.0           (optional) <a href="https://pip-services3-java.github.io/pip-services3-components-java/org/pipservices3/components/log/ILogger.html">ILogger</a> components to pass log messages
 * <li>*:discovery:*:*:1.0        (optional) <a href="https://pip-services3-java.github.io/pip-services3-components-java/org/pipservices3/components/connect/IDiscovery.html">IDiscovery</a> services
 * <li>*:credential-store:*:*:1.0 (optional) Credential stores to resolve credentials
 * </ul>
 * <p>
 * ### Example ###
 * <pre>
 * {@code
 * class MyMongoDbPersistence extends MongoDbPersistence<MyData> {
 *    
 *   public MyMongoDbPersistence() {
 *       super("mydata", MyData.class);
 *   }
 * 
 *   public MyData getByName(String correlationId, String name) {
 *   	Bson filter = Filters.eq("name", name);
 *   	MyData item = _collection.find(filter).first();
 *   	return item;
 *   } 
 * 
 *   public MyData set(String correlatonId, MyData item) {
 *       Bson filter = Filters.eq("name", item.getName());
 *       
 *       FindOneAndReplaceOptions options = new FindOneAndReplaceOptions();
 *       options.returnDocument(ReturnDocument.AFTER);
 *       options.upsert(true);
 *       
 *       MyData result = _collection.findOneAndReplace(filter, item, options);
 *       return result;
 *   }
 * 
 * }
 * 
 * MyMongoDbPersistence persistence = new MyMongoDbPersistence();
 * persistence.configure(ConfigParams.fromTuples(
 *     "host", "localhost",
 *     "port", 27017
 * ));
 * 
 * persitence.open("123");
 * MyData mydata = new MyData("ABC");
 * persistence.set("123", mydata); 
 * persistence.getByName("123", "ABC");
 * System.out.println(item);                   // Result: { name: "ABC" }
 * }
 * </pre>
 * 
 */
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
	/**
	 * The collection name.
	 */
	protected String _collectionName;
	/**
	 * The connection resolver.
	 */
	protected MongoDbConnectionResolver _connectionResolver = new MongoDbConnectionResolver();
	/**
	 * The configuration options.
	 */
	protected ConfigParams _options = new ConfigParams();
	protected Object _lock = new Object();

	/**
	 * The MongoDB connection object.
	 */
	protected MongoClient _connection;

	/**
	 * The MongoDB database name.
	 */
	protected MongoDatabase _database;
	/**
	 * The MongoDB colleciton object.
	 */
	protected MongoCollection<T> _collection;
	/**
	 * The default class to cast any documents returned from the database into
	 */
	protected Class<T> _documentClass;

	/**
	 * The logger.
	 */
	protected CompositeLogger _logger = new CompositeLogger();

	/**
	 * Creates a new instance of the persistence component.
	 * 
	 * @param collectionName    (optional) a collection name.
	 * @param documentClass the default class to cast any documents returned from
	 *                      the database into
	 */
	public MongoDbPersistence(String collectionName, Class<T> documentClass) {
		if (collectionName == null)
			throw new NullPointerException(collectionName);

		_collectionName = collectionName;
		_documentClass = documentClass;
	}

	/**
	 * Configures component by passing configuration parameters.
	 * 
	 * @param config configuration parameters to be set.
	 */
	public void configure(ConfigParams config) {
		config = config.setDefaults(_defaultConfig);

		_connectionResolver.configure(config);

		_collectionName = config.getAsStringWithDefault("collection", _collectionName);

		_options = _options.override(config.getSection("options"));
	}

	/**
	 * Sets references to dependent components.
	 * 
	 * @param references references to locate the component dependencies.
	 */
	public void setReferences(IReferences references) {
		_logger.setReferences(references);
		_connectionResolver.setReferences(references);
	}

	/**
	 * Checks if the component is opened.
	 * 
	 * @return true if the component has been opened and false otherwise.
	 */
	public boolean isOpen() {
		return _collection != null;
	}

	/**
	 * Checks if the component is opened.
	 * 
	 * @param correlationId (optional) transaction id to trace execution through
	 *                      call chain.
	 * @throws InvalidStateException when operation cannot be performed.
	 */
	protected void checkOpened(String correlationId) throws InvalidStateException {
		if (!isOpen()) {
			throw new InvalidStateException(correlationId, "NOT_OPENED",
					"Operation cannot be performed because the component is closed");
		}
	}

	/**
	 * Opens the component.
	 * 
	 * @param correlationId (optional) transaction id to trace execution through
	 *                      call chain.
	 * @throws ApplicationException when error occured.
	 */
	public void open(String correlationId) throws ApplicationException {
        String uri = _connectionResolver.resolve(correlationId);

        _logger.trace(correlationId, "Connecting to mongodb");
        
        try {
        	MongoClientURI clientUri = new MongoClientURI(uri);
        	String databaseName = clientUri.getDatabase();
        	
        	_connection = new MongoClient(clientUri);

            PojoCodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();
            CodecRegistry pojoCodecRegistry = CodecRegistries.fromRegistries(
        		// Custom codecs for unsupported types
        		CodecRegistries.fromCodecs(
    				new ZonedDateTimeStringCodec(), 
    				new LocalDateTimeStringCodec(),
    				new LocalDateStringCodec(),
    				new DurationInt64Codec()
				),
        		MongoClient.getDefaultCodecRegistry(),
        		// POJO codecs to allow object serialization
        		CodecRegistries.fromProviders(pojoCodecProvider)
    		);
        	_database = _connection.getDatabase(databaseName).withCodecRegistry(pojoCodecRegistry);

            _collection =  (MongoCollection<T>)_database.getCollection(_collectionName, _documentClass);
//            if (_collection == null)
//            	_database.createCollection(_collectionName);
//            _collection =  (MongoCollection<T>)_database.getCollection(_collectionName, _documentClass);

			_logger.debug(correlationId, "Connected to mongodb database %s, collection %s", databaseName,
					_collectionName);
		} catch (Exception ex) {
			_connection = null;

			throw new ConnectionException(correlationId, "Connection to mongodb failed", ex.toString());
		}
	}

	/**
	 * Closes component and frees used resources.
	 * 
	 * @param correlationId (optional) transaction id to trace execution through
	 *                      call chain.
	 * @throws ApplicationException when error occured.
	 */
	public void close(String correlationId) throws ApplicationException {
		if (_connection != null) {
			_connection.close();

			_connection = null;
			_database = null;
			_collection = null;
		}
	}

	/**
	 * Clears component state.
	 * 
	 * @param correlationId (optional) transaction id to trace execution through
	 *                      call chain.
	 * @throws ApplicationException when error occured.
	 */
	@Override
	public void clear(String correlationId) throws ApplicationException {
		checkOpened(correlationId);

		_collection.drop();
	}

}
