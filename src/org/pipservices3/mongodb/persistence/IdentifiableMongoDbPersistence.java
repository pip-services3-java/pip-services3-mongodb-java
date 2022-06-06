package org.pipservices3.mongodb.persistence;

import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.pipservices3.commons.convert.JsonConverter;
import org.pipservices3.commons.data.AnyValueMap;
import org.pipservices3.commons.data.IIdentifiable;
import org.pipservices3.commons.errors.ApplicationException;
import org.pipservices3.data.IGetter;
import org.pipservices3.data.ISetter;
import org.pipservices3.data.IWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Abstract persistence component that stores data in MongoDB
 * and implements a number of CRUD operations over data items with unique ids.
 * The data items must implement <a href="https://pip-services3-java.github.io/pip-services3-components-java/org/pipservices3/commons/data/IIdentifiable.html">IIdentifiable</a> interface.
 * <p>
 * In basic scenarios child classes shall only override <code>getPageByFilter()</code>,
 * <code>getListByFilter()</code> or <code>deleteByFilter()</code> operations with specific filter function.
 * All other operations can be used out of the box.
 * <p>
 * In complex scenarios child classes can implement additional operations by
 * accessing <code>this._collection</code> and <code>this._model</code> properties.
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
 *   <li>connect_timeout:           (optional) connection timeout in milliseconds (default: 5000)
 *   <li>socket_timeout:            (optional) socket timeout in milliseconds (default: 360000)
 *   <li>auto_reconnect:            (optional) enable auto reconnection (default: true)
 *   <li>reconnect_interval:        (optional) reconnection interval in milliseconds (default: 1000)
 *   <li>max_page_size:             (optional) maximum page size (default: 100)
 *   <li>replica_set:               (optional) name of replica set
 *   <li>ssl:                       (optional) enable SSL connection (default: false)
 *   <li>auth_source:               (optional) authentication source
 *   <li>auth_user:                 (optional) authentication user name
 *   <li>auth_password:             (optional) authentication user password
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
 * class MyMongoDbPersistence extends MongoDbPersistence<MyData, String> {
 *
 *   public MyMongoDbPersistence() {
 *       super("mydata", MyData.class);
 *   }
 *
 *   private Bson composeFilter(FilterParams filter) {
 *       filter = filter != null ? filter : new FilterParams();
 *       ArrayList<Bson> filters = new ArrayList<Bson>();
 *       String name = filter.getAsNullableString('name');
 *       if (name != null)
 *           filters.add(Filters.eq("name", name));
 *       return Filters.and(filters);
 *   }
 *
 *   public getPageByFilter(String correlationId, FilterParams filter, PagingParams paging) {
 *       super.getPageByFilter(correlationId, this.composeFilter(filter), paging, null, null);
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
 *
 * persistence.create("123", new MyData("1", "ABC"));
 * DataPage<MyData> mydata = persistence.getPageByFilter(
 *         "123",
 *         FilterParams.fromTuples("name", "ABC"),
 *         null,
 *         null);
 * System.out.println(mydata.getData().toString());          // Result: { id: "1", name: "ABC" }
 *
 * persistence.deleteById("123", "1");
 * ...
 * }
 * </pre>
 */
public class IdentifiableMongoDbPersistence<T extends IIdentifiable<K>, K extends Comparable<K>> extends MongoDbPersistence<T>
        implements IWriter<T, K>, IGetter<T, K>, ISetter<T> {

    /**
     * Flag to turn on automated string ID generation
     */
    protected boolean _autoGenerateId = true;

    /**
     * Creates a new instance of the persistence component.
     *
     * @param collectionName (optional) a collection name.
     * @param documentClass  the default class to cast any documents returned from
     *                       the database into
     */
    public IdentifiableMongoDbPersistence(String collectionName, Class<T> documentClass) {
        super(collectionName, documentClass);
    }

    /**
     * Converts the given object from the public partial format.
     *
     * @param value the object to convert from the public partial format.
     * @return the initial object.
     */
    protected Document convertFromPublicPartial(Object value) {
        return this.convertFromPublic(value);
    }

    /**
     * Gets a list of data items retrieved by given unique ids.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param ids           ids of data items to be retrieved
     * @return a data list.
     */
    public List<T> getListByIds(String correlationId, List<K> ids) {
        var filter = new Document("_id", new Document("$in", ids));
        return this.getListByFilter(correlationId, filter, null, null);
    }

    /**
     * Gets a list of data items retrieved by given unique ids.
     *
     * @param correlationId (optional) transaction id to trace execution through
     *                      call chain.
     * @param ids           ids of data items to be retrieved
     * @return a data list of results by ids.
     */
    public List<T> getListByIds(String correlationId, K[] ids) {
        var filter = new Document("_id", new Document("$in", Arrays.stream(ids).toList()));
        return getListByFilter(correlationId, filter, null, null);
    }

    /**
     * Gets a data item by its unique id.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param id            an id of data item to be retrieved.
     * @return the found data item.
     */
    public T getOneById(String correlationId, K id) {
        var filter = new Document("_id", id);

        var item = convertToPublic(this._collection.find(filter).first());

        if (item == null)
            this._logger.trace(correlationId, "Nothing found from %s with id = %s", this._collectionName, id);
        else
            this._logger.trace(correlationId, "Retrieved from %s with id = %s", this._collectionName, id);

        return item;
    }

    /**
     * Creates a data item.
     *
     * @param correlationId (optional) transaction id to trace execution through
     *                      call chain.
     * @param item          an item to be created.
     * @return created item.
     */
    @Override
    public T create(String correlationId, T item) {
        if (item == null) return null;

        // clone object
        T newItem;
        try {
            newItem = JsonConverter.fromJson(_documentClass, JsonConverter.toJson(item));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        // Auto generate id
        if (newItem.getId() == null && this._autoGenerateId)
            newItem.setId(newItem.withGeneratedId());

        return super.create(correlationId, item);
    }

    /**
     * Sets a data item. If the data item exists it updates it, otherwise it create
     * a new data item.
     *
     * @param correlationId (optional) transaction id to trace execution through
     *                      call chain.
     * @param item          a item to be set.
     * @return updated item.
     */
    public T set(String correlationId, T item) {
        if (item == null)
            return null;

        // Copy object
        T newItem;
        try {
            newItem = JsonConverter.fromJson(_documentClass, JsonConverter.toJson(item));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        // Auto generate id
        if (newItem.getId() == null && this._autoGenerateId)
            newItem.setId(newItem.withGeneratedId());

        var filter = new Document("_id", newItem.getId());

        var options = new FindOneAndUpdateOptions();

        options.upsert(true);
        options.returnDocument(ReturnDocument.AFTER);

        var update = convertFromPublic(newItem);

        var result = this._collection.findOneAndUpdate(filter, update, options);

        if (result != null && !result.isEmpty())
            this._logger.trace(correlationId, "Set in %s with id = %s", this._collectionName, item.getId());

        newItem = result != null ? this.convertToPublic(result) : null;
        return newItem;
    }

    /**
     * Updates a data item.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param item          an item to be updated.
     * @return the updated item.
     */
    public T update(String correlationId, T item) {
        if (item == null || item.getId() == null)
            return null;

        // Copy object
        T newItem;
        try {
            newItem = JsonConverter.fromJson(_documentClass, JsonConverter.toJson(item));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        var update = this.convertFromPublic(newItem);
        update = new Document("$set", update);

        var options = new FindOneAndUpdateOptions();
        var filter = new Document("_id", newItem.getId());

        options.returnDocument(ReturnDocument.AFTER);

        var result = this._collection.findOneAndUpdate(filter, update, options);

        this._logger.trace(correlationId, "Updated in %s with id = %s", this._collectionName, item.getId());

        newItem = result != null ? this.convertToPublic(result) : null;
        return newItem;
    }

    /**
     * Updates only few selected fields in a data item.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param id            an id of data item to be updated.
     * @param data          a map with fields to be updated.
     * @return the updated item.
     */
    public T updatePartially(String correlationId, K id, AnyValueMap data) {
        if (data == null || id == null)
            return null;

        var newItem = data.getAsObject();

        var update = this.convertFromPublicPartial(newItem);
        update = new Document("$set", update);

        var options = new FindOneAndUpdateOptions();
        options.returnDocument(ReturnDocument.AFTER);

        var filter = new Document("_id", id);

        var result = this._collection.findOneAndUpdate(filter, update, options);

        this._logger.trace(correlationId, "Updated partially in %s with id = %s", this._collectionName, id);

        return result != null ? this.convertToPublic(result) : null;
    }

    /**
     * Deleted a data item by it's unique id.
     *
     * @param correlationId (optional) transaction id to trace execution through
     *                      call chain.
     * @param id            an id of the item to be deleted
     * @return deleted item.
     */
    public T deleteById(String correlationId, K id) {
        var filter = new Document("_id", id);

        var result = this._collection.findOneAndDelete(filter);

        this._logger.trace(correlationId, "Deleted from %s with id = %s", this._collectionName, id);

        return result != null ? this.convertToPublic(result) : null;
    }

    /**
     * Deletes multiple data items by their unique ids.
     *
     * @param correlationId (optional) transaction id to trace execution through
     *                      call chain.
     * @param ids           ids of data items to be deleted.
     */
    public void deleteByIds(String correlationId, K[] ids) {

        Bson filter = new Document("_id", new Document("$in", Arrays.stream(ids).toList()));

        DeleteResult result = _collection.deleteMany(filter);

        _logger.trace(correlationId, "Deleted %d from %s", result.getDeletedCount(), _collectionName);
    }

    /**
     * Deletes multiple data items by their unique ids.
     *
     * @param correlationId (optional) transaction id to trace execution through
     *                      call chain.
     * @param ids           ids of data items to be deleted.
     */
    public void deleteByIds(String correlationId, List<K> ids) {

        Bson filter = new Document("_id", new Document("$in", ids));

        DeleteResult result = _collection.deleteMany(filter);

        _logger.trace(correlationId, "Deleted %d from %s", result.getDeletedCount(), _collectionName);
    }
}
