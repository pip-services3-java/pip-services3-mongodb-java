package org.pipservices3.mongodb.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.pipservices3.commons.config.ConfigParams;
import org.pipservices3.commons.config.IConfigurable;
import org.pipservices3.commons.convert.JsonConverter;
import org.pipservices3.commons.data.DataPage;
import org.pipservices3.commons.data.PagingParams;
import org.pipservices3.commons.errors.ApplicationException;
import org.pipservices3.commons.errors.ConfigException;
import org.pipservices3.commons.errors.ConnectionException;
import org.pipservices3.commons.errors.InvalidStateException;
import org.pipservices3.commons.refer.*;
import org.pipservices3.commons.run.ICleanable;
import org.pipservices3.commons.run.IOpenable;
import org.pipservices3.components.log.CompositeLogger;
import org.pipservices3.mongodb.connect.MongoDbConnection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
 */
public class MongoDbPersistence<T> implements IReferenceable, IUnreferenceable, IConfigurable, IOpenable, ICleanable {

    private final ConfigParams _defaultConfig = ConfigParams.fromTuples(
            "collection", null,
            "dependencies.connection", "*:connection:mongodb:*:1.0",

            // connections.*
            // credential.*

            "options.max_pool_size", 2,
            "options.keep_alive", 1,
            "options.connect_timeout", 5000,
            "options.auto_reconnect", true,
            "options.max_page_size", 100,
            "options.debug", true
    );

    private ConfigParams _config;
    private IReferences _references;
    private boolean _opened;
    private boolean _localConnection;
    private List<MongoDbIndex> _indexes = new ArrayList<>();

    /**
     * The dependency resolver.
     */
    protected DependencyResolver _dependencyResolver = new DependencyResolver(_defaultConfig);

    /**
     * The logger.
     */
    protected CompositeLogger _logger = new CompositeLogger();

    /**
     * The MongoDB connection component.
     */
    protected MongoDbConnection _connection;

    /**
     * The MongoDB connection object.
     */
    protected MongoClient _client;

    /**
     * The MongoDB database name.
     */
    protected String _databaseName;

    /**
     * The collection name.
     */
    protected String _collectionName;

    /**
     * The MongoDb database object.
     */
    protected MongoDatabase _db;

    /**
     * The MongoDB colleciton object.
     */
    protected MongoCollection<Document> _collection;

    protected long _maxPageSize = 100;

    /**
     * The default class to cast any documents returned from the database into
     */
    protected Class<T> _documentClass;


    /**
     * Creates a new instance of the persistence component.
     *
     * @param collectionName (optional) a collection name.
     * @param documentClass  the default class to cast any documents returned from
     *                       the database into
     */
    public MongoDbPersistence(String collectionName, Class<T> documentClass) {
        if (collectionName == null)
            throw new NullPointerException("collectionName is null");

        _collectionName = collectionName;
        _documentClass = documentClass;
    }

    /**
     * Configures component by passing configuration parameters.
     *
     * @param config configuration parameters to be set.
     */
    @Override
    public void configure(ConfigParams config) throws ConfigException {
        config = config.setDefaults(_defaultConfig);
        this._config = config;

        this._dependencyResolver.configure(config);

        this._collectionName = config.getAsStringWithDefault("collection", this._collectionName);
        this._maxPageSize = config.getAsLongWithDefault("options.max_page_size", this._maxPageSize);
    }

    /**
     * Sets references to dependent components.
     *
     * @param references references to locate the component dependencies.
     */
    @Override
    public void setReferences(IReferences references) throws ReferenceException, ConfigException {
        this._references = references;
        this._logger.setReferences(references);

        // Get connection
        this._dependencyResolver.setReferences(references);
        this._connection = this._dependencyResolver.getOneOptional(MongoDbConnection.class, "connection");
        // Or create a local one
        if (this._connection == null) {
            this._connection = this.createConnection();
            this._localConnection = true;
        } else {
            this._localConnection = false;
        }
    }

    /**
     * Unsets (clears) previously set references to dependent components.
     */
    @Override
    public void unsetReferences() {
        this._connection = null;
    }

    /**
     * Checks if the component is opened.
     *
     * @return true if the component has been opened and false otherwise.
     */
    @Override
    public boolean isOpen() {
        return this._opened;
    }

    private MongoDbConnection createConnection() throws ConfigException, ReferenceException {
        var connection = new MongoDbConnection();

        if (this._config != null)
            connection.configure(this._config);

        if (this._references != null)
            connection.setReferences(this._references);

        return connection;
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
     * Adds index definition to create it on opening
     *
     * @param keys    index keys (fields)
     * @param options index options
     */
    protected void ensureIndex(Bson keys, IndexOptions options) {
        if (keys == null) return;
        this._indexes.add(new MongoDbIndex(keys, options));
    }

    /**
     * Clears all auto-created objects
     */
    protected void clearSchema() {
        this._indexes = new ArrayList<>();
    }

    /**
     * Defines database schema via auto create objects or convenience methods.
     */
    protected void defineSchema() {
        // Todo: override in child classes
    }

    /**
     * Converts object value from internal to public format.
     *
     * @param value an object in internal format to convert.
     * @return converted object in public format.
     */
    protected T convertToPublic(Document value) {
        try {
            if (value == null || value.isEmpty()) return null;
            if (value.containsKey("_id")) {
                value.put("id", value.get("_id"));
                value.remove("_id");
            }

            return JsonConverter.fromJson(_documentClass, value.toJson());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Convert object value from public to internal format.
     *
     * @param value an object in public format to convert.
     * @return converted object in internal format.
     */
    protected Document convertFromPublic(Object value) {
        var mongoDoc = new Document();

        if (value != null) {
            String json;

            try {
                json = JsonConverter.toJson(value);
            } catch (JsonProcessingException ex) {
                throw new RuntimeException(ex);
            }

            mongoDoc = Document.parse(json);

            if (mongoDoc.containsKey("id") && mongoDoc.get("id") != null) {
                mongoDoc.put("_id", mongoDoc.get("id"));
                mongoDoc.remove("id");
            }
        }

        return mongoDoc;
    }

    /**
     * Opens the component.
     *
     * @param correlationId (optional) transaction id to trace execution through
     *                      call chain.
     * @throws ApplicationException when error occured.
     */
    public void open(String correlationId) throws ApplicationException {
        if (this._opened) return;

        if (this._connection == null) {
            this._connection = this.createConnection();
            this._localConnection = true;
        }

        if (this._localConnection)
            this._connection.open(correlationId);

        if (this._connection == null) {
            throw new InvalidStateException(correlationId, "NO_CONNECTION", "MongoDB connection is missing");
        }

        if (!this._connection.isOpen())
            throw new ConnectionException(correlationId, "CONNECT_FAILED", "MongoDB connection is not opened");

        this._opened = false;

        this._client = this._connection.getConnection();
        this._db = this._connection.getDatabase();
        this._databaseName = this._connection.getDatabaseName();

        try {
            var collection = this._db.getCollection(_collectionName);

            // Define database schema
            this.defineSchema();

            // Recreate indexes
            for (var index : _indexes) {
                var indexName = collection.createIndex(index.keys, index.options);

                var options = index.options != null ? index.options : new IndexOptions();
                indexName = !indexName.isEmpty() ? indexName : options.getName();
                this._logger.debug(correlationId, "Created index %s for collection %s", indexName, this._collectionName);
            }

            this._opened = true;
            this._collection = collection;
            this._logger.debug(correlationId, "Connected to mongodb database %s, collection %s", this._databaseName, this._collectionName);
        } catch (Exception ex) {
            this._db = null;
            this._client = null;
            throw new ConnectionException(correlationId, "CONNECT_FAILED", "Connection to mongodb failed").withCause(ex);
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
        if (!this._opened)
            return;

        if (this._connection == null)
            throw new InvalidStateException(correlationId, "NO_CONNECTION", "MongoDb connection is missing");

        if (this._localConnection)
            this._connection.close(correlationId);

        this._opened = false;
        this._client = null;
        this._db = null;
        this._collection = null;
    }

    /**
     * Clears component state.
     *
     * @param correlationId (optional) transaction id to trace execution through
     *                      call chain.
     * @throws ApplicationException when error occured.
     */
    @Override
    public void clear(String correlationId) throws ApplicationException {// Return error if collection is not set
        if (this._collectionName == null) {
            try {
                throw new Exception("Collection name is not defined");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        checkOpened(correlationId);

        _collection.drop();
    }

    /**
     * Gets a page of data items retrieved by a given filter and sorted according to sort parameters.
     * <p>
     * This method shall be called by a public getPageByFilter method from child class that
     * receives FilterParams and converts them into a filter function.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param filter        (optional) a filter JSON object
     * @param paging        (optional) paging parameters
     * @param sort          (optional) sorting JSON object
     * @param select        (optional) projection JSON object
     * @return a data page.
     */
    protected DataPage<T> getPageByFilter(String correlationId, Bson filter, PagingParams paging,
                                          Bson sort, Bson select) {

        // Adjust max item count based on configuration
        paging = paging != null ? paging : new PagingParams();
        var skip = paging.getSkip(-1);
        var take = paging.getTake(this._maxPageSize);
        var pagingEnabled = paging.hasTotal();

        // Configure options
        var options = new BasicDBObject();

        List<T> items = new ArrayList<>();

        var res = _collection.find(options)
                .limit((int) take)
                .skip((int) skip)
                .sort(sort)
                .projection(select);

        for (var item : res)
            items.add(convertToPublic(item));

        if (!items.isEmpty())
            this._logger.trace(correlationId, "Retrieved %d from %s", items.size(), this._collectionName);

        Long count = null;

        if (pagingEnabled)
            count = _collection.countDocuments(filter);

        return new DataPage<T>(items, count);
    }

    /**
     * Gets a number of data items retrieved by a given filter.
     * <p>
     * This method shall be called by a public getCountByFilter method from child class that
     * receives FilterParams and converts them into a filter function.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param filter        (optional) a filter JSON object
     * @return a number of filtered items.
     */
    protected Long getCountByFilter(String correlationId, Bson filter) {
        Long count = _collection.countDocuments(filter);

        this._logger.trace(correlationId, "Counted %d items in %s", count, this._collectionName);

        return count;
    }

    /**
     * Gets a list of data items retrieved by a given filter and sorted according to sort parameters.
     * <p>
     * This method shall be called by a public getListByFilter method from child class that
     * receives FilterParams and converts them into a filter function.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param filter        (optional) a filter JSON object
     * @param sort          (optional) sorting JSON object
     * @param select        (optional) projection JSON object
     * @return a filtered data list.
     */
    protected List<T> getListByFilter(String correlationId, Bson filter, Bson sort, Bson select) {
        // Configure options
        var options = new BasicDBObject();

        if (sort != null) options.put("sort", sort);

        List<T> items = new ArrayList<>();

        var res = _collection.find(filter).filter(options).projection(select);

        for (var item : res)
            items.add(convertToPublic(item));

        if (!items.isEmpty())
            this._logger.trace(correlationId, "Retrieved %d from %s", items.size(), this._collectionName);

        return items;
    }

    /**
     * Gets a random item from items that match to a given filter.
     * <p>
     * This method shall be called by a public getOneRandom method from child class that
     * receives FilterParams and converts them into a filter function.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param filter        (optional) a filter JSON object
     * @return a random item.
     */
    protected T getOneRandom(String correlationId, Bson filter) {
        var count = _collection.countDocuments(filter);

        var pos = (int) (Math.random() * count);
        var options = new BasicDBObject();

        List<T> items = new ArrayList<>();

        var res = _collection.find(filter).filter(options).skip(Math.max(pos, 0)).limit(1);

        for (var item : res)
            items.add(convertToPublic(item));

        var item = (items.size() > 0) ? items.get(0) : null;

        if (item == null)
            this._logger.trace(correlationId, "Random item wasn't found from %s", this._collectionName);
        else
            this._logger.trace(correlationId, "Retrieved random item from %s", this._collectionName);

        return item;
    }

    /**
     * Creates a data item.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param item          an item to be created.
     * @return the created item.
     */
    public T create(String correlationId, T item) {
        if (item == null)
            return null;

        var newItem = this.convertFromPublic(item);

        _collection.insertOne(newItem);

        this._logger.trace(correlationId, "Created in %s with id = %s", this._collectionName, newItem.get("_id"));

        return item;
    }


    /**
     * Deletes data items that match to a given filter.
     * <p>
     * This method shall be called by a public deleteByFilter method from child class that
     * receives FilterParams and converts them into a filter function.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param filter        (optional) a filter JSON object.
     */
    public void deleteByFilter(String correlationId, Bson filter) {
        var result = _collection.deleteMany(filter);

        var count = result.getDeletedCount();
        this._logger.trace(correlationId, "Deleted %d items from %s", count, this._collectionName);

    }
}
