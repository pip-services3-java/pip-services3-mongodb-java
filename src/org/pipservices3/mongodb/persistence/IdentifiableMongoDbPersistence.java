package org.pipservices3.mongodb.persistence;

import java.util.*;

import org.bson.*;
import org.bson.conversions.*;
import org.pipservices3.commons.config.*;
import org.pipservices3.commons.data.*;
import org.pipservices3.commons.errors.*;

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.client.result.*;

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
public class IdentifiableMongoDbPersistence<T extends IIdentifiable<K>, K> extends MongoDbPersistence<T>
/* implements IWriter<T, K>, IGetter<T, K>, ISetter<T> */ {

	protected int _maxPageSize = 100;

	/**
	 * Creates a new instance of the persistence component.
	 * 
	 * @param collectionName    (optional) a collection name.
	 * @param documentClass the default class to cast any documents returned from
	 *                      the database into
	 */
	public IdentifiableMongoDbPersistence(String collectionName, Class<T> documentClass) {
		super(collectionName, documentClass);
	}

	/**
	 * Configures component by passing configuration parameters.
	 * 
	 * @param config configuration parameters to be set.
	 */
	public void configure(ConfigParams config) {
		super.configure(config);

		_maxPageSize = config.getAsIntegerWithDefault("options.max_page_size", _maxPageSize);
	}

	/**
	 * Gets a page of data items retrieved by a given filter and sorted according to
	 * sort parameters.
	 * 
	 * This method shall be called by a public getPageByFilter method from child
	 * class that receives FilterParams and converts them into a filter function.
	 * 
	 * @param correlationId (optional) transaction id to trace execution through
	 *                      call chain.
	 * @param filter        (optional) a filter JSON object
	 * @param paging        (optional) paging parameters
	 * @param sort          (optional) sorting JSON object
	 * @return data page of results by filter.
	 * @throws ApplicationException when error occured.
	 */
	protected DataPage<T> getPageByFilter(String correlationId, Bson filter, PagingParams paging, Bson sort)
			throws ApplicationException {

		checkOpened(correlationId);

		filter = filter != null ? filter : new Document();
		FindIterable<T> query = _collection.find(filter);

		if (sort != null)
			query = query.sort(sort);

		paging = paging != null ? paging : new PagingParams();
		int skip = (int) paging.getSkip(0);
		int take = (int) paging.getTake(_maxPageSize);
		query = query.skip(skip).limit(take);

		List<T> items = new ArrayList<T>();
		query.forEach(new Block<T>() {
			@Override
			public void apply(final T item) {
				items.add(item);
			}
		});

		Long count = paging.hasTotal() ? _collection.count(filter) : null;

		_logger.trace(correlationId, "Retrieved %d from %s", items.size(), _collectionName);

		return new DataPage<T>(items, count);
	}

	/**
	 * Gets a list of data items retrieved by a given filter and sorted according to
	 * sort parameters.
	 * 
	 * This method shall be called by a public getListByFilter method from child
	 * class that receives FilterParams and converts them into a filter function.
	 * 
	 * @param correlationId (optional) transaction id to trace execution through
	 *                      call chain.
	 * @param filter        (optional) a filter JSON object
	 * @param sort          (optional) sorting JSON object
	 * @return a data list of result by filter.
	 * @throws ApplicationException when error occured.
	 */
	protected List<T> getListByFilter(String correlationId, Bson filter, Bson sort) throws ApplicationException {

		checkOpened(correlationId);

		filter = filter != null ? filter : new Document();
		FindIterable<T> query = _collection.find(filter);

		if (sort != null)
			query = query.sort(sort);

		List<T> items = new ArrayList<T>();
		query.forEach(new Block<T>() {
			@Override
			public void apply(final T item) {
				items.add(item);
			}
		});

		_logger.trace(correlationId, "Retrieved %d from %s", items.size(), _collectionName);

		return items;
	}

	/**
	 * Gets a list of data items retrieved by given unique ids.
	 * 
	 * @param correlationId (optional) transaction id to trace execution through
	 *                      call chain.
	 * @param ids           ids of data items to be retrieved
	 * @return a data list of results by ids.
	 * @throws ApplicationException when error occured.
	 */
	public List<T> getListByIds(String correlationId, K[] ids) throws ApplicationException {

		Bson filter = Filters.in("_id", ids);

		return getListByFilter(correlationId, filter, null);
	}

	/**
	 * Gets a data item by its unique id.
	 * 
	 * @param correlationId (optional) transaction id to trace execution through
	 *                      call chain.
	 * @param id            an id of data item to be retrieved.
	 * @return a data item by id.
	 * @throws ApplicationException when error occured.
	 */
	public T getOneById(String correlationId, K id) throws ApplicationException {

		checkOpened(correlationId);

		Bson filter = Filters.eq("_id", id);

		T item = _collection.find(filter).first();

		if (item == null)
			_logger.trace(correlationId, "Nothing found from %s with id = %s", _collectionName, id.toString());
		else
			_logger.trace(correlationId, "Retrieved from %s with id = %s", _collectionName, id.toString());

		return item;
	}

	/**
	 * Gets a random item from items that match to a given filter.
	 * 
	 * This method shall be called by a public getOneRandom method from child class
	 * that receives FilterParams and converts them into a filter function.
	 * 
	 * @param correlationId (optional) transaction id to trace execution through
	 *                      call chain.
	 * @param filter        (optional) a filter JSON object
	 * @return a random item by filter.
	 * @throws ApplicationException when error occured.
	 */
	protected T getOneRandom(String correlationId, Bson filter) throws ApplicationException {

		checkOpened(correlationId);

		filter = filter != null ? filter : new Document();
		int count = (int) _collection.count(filter);

		if (count <= 0)
			return null;

		int randomIndex = new Random().nextInt(count - 1);
		T item = _collection.find(filter).skip(randomIndex).first();

		_logger.trace(correlationId, "Retrieved randomly from %s with id = %s", _collectionName,
				item.getId().toString());

		return item;
	}

	/**
	 * Creates a data item.
	 * 
	 * @param correlationId (optional) transaction id to trace execution through
	 *                       call chain.
	 * @param item           an item to be created.
	 * @return created item.
	 * @throws ApplicationException when error occured.
	 */
	public T сreate(String correlationId, T item) throws ApplicationException {

		checkOpened(correlationId);

		if (item instanceof IStringIdentifiable && item.getId() == null)
			((IStringIdentifiable) item).setId(IdGenerator.nextLong());

		_collection.insertOne(item);

		_logger.trace(correlationId, "Created in %s with id = %s", _collectionName, item.getId().toString());

		return item;
	}

	/**
	 * Sets a data item. If the data item exists it updates it, otherwise it create
	 * a new data item.
	 * 
	 * @param correlationId (optional) transaction id to trace execution through
	 *                      call chain.
	 * @param newItem          a item to be set.
	 * @return updated item.
	 * @throws ApplicationException when error occured.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public T set(String correlationId, T newItem) throws ApplicationException {

		checkOpened(correlationId);

		if (newItem instanceof IIdentifiable && newItem.getId() == null)
			((IIdentifiable) newItem).setId(IdGenerator.nextLong());

		Bson filter = Filters.eq("_id", newItem.getId());

		FindOneAndReplaceOptions options = new FindOneAndReplaceOptions();
		options.returnDocument(ReturnDocument.AFTER);
		options.upsert(true);

		T result = _collection.findOneAndReplace(filter, newItem, options);

		_logger.trace(correlationId, "Set in %s with id = %s", _collectionName, newItem.getId().toString());

		return result;
	}

	/**
	 * Updates a data item.
	 * 
	 * @param correlationId (optional) transaction id to trace execution through
	 *                       call chain.
	 * @param newItem           an item to be updated.
	 * @return updated item.
	 * @throws ApplicationException when error occured.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public T update(String correlationId, T newItem) throws ApplicationException {

		checkOpened(correlationId);

		if (newItem instanceof IIdentifiable && newItem.getId() == null)
			((IIdentifiable) newItem).setId(IdGenerator.nextLong());

		Bson filter = Filters.eq("_id", newItem.getId());

		FindOneAndReplaceOptions options = new FindOneAndReplaceOptions();
		options.returnDocument(ReturnDocument.AFTER);
		options.upsert(false);

		T result = _collection.findOneAndReplace(filter, newItem, options);

		_logger.trace(correlationId, "Update in %s with id = %s", _collectionName, newItem.getId().toString());

		return result;
	}

	/**
	 * Updates only few selected fields in a data item.
	 * 
	 * @param correlationId (optional) transaction id to trace execution through
	 *                       call chain.
	 * @param id             an id of data item to be updated.
	 * @param data           a map with fields to be updated.
	 * @return updated item.
	 * @throws ApplicationException when error occured.
	 */
	public T updatePartially(String correlationId, K id, AnyValueMap data) throws ApplicationException {

		checkOpened(correlationId);

		if (id == null && data == null)
			return null;

		// Define filter
		Bson filter = Filters.eq("_id", id);

		// Define update set
		data = data != null ? data : new AnyValueMap();

		List<Bson> updateDefinitions = new ArrayList<Bson>();
		for (String key : data.keySet()) {
			updateDefinitions.add(Updates.set(key, data.get(key)));
		}

		Bson update = Updates.combine(updateDefinitions);

		// Define options
		FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
		options.returnDocument(ReturnDocument.AFTER);
		options.upsert(false);

		T result = _collection.findOneAndUpdate(filter, update, options);

		_logger.trace(correlationId, "Updated in %s", _collectionName);

		return result;
	}

	/**
	 * Deleted a data item by it's unique id.
	 * 
	 * @param correlationId (optional) transaction id to trace execution through
	 *                       call chain.
	 * @param id             an id of the item to be deleted
	 * @return deleted item.
	 * @throws ApplicationException when error occured.
	 */
	public T deleteById(String correlationId, K id) throws ApplicationException {

		checkOpened(correlationId);

		Bson filter = Filters.eq("_id", id);

		FindOneAndDeleteOptions options = new FindOneAndDeleteOptions();

		T result = _collection.findOneAndDelete(filter, options);

		_logger.trace(correlationId, "Deleted from %s with id = %s", _collectionName, id.toString());

		return result;
	}

	/**
	 * Deletes data items that match to a given filter.
	 * 
	 * This method shall be called by a public deleteByFilter method from child
	 * class that receives FilterParams and converts them into a filter function.
	 * 
	 * @param correlationId (optional) transaction id to trace execution through
	 *                      call chain.
	 * @param filter        (optional) a filter JSON object.
	 * @throws ApplicationException when error occured.
	 */
	protected void deleteByFilter(String correlationId, Bson filter) throws ApplicationException {

		checkOpened(correlationId);

		filter = filter != null ? filter : new Document();
		DeleteResult result = _collection.deleteMany(filter);

		_logger.trace(correlationId, "Deleted %d from %s", result.getDeletedCount(), _collectionName);
	}

	/**
	 * Deletes multiple data items by their unique ids.
	 * 
	 * @param correlationId (optional) transaction id to trace execution through
	 *                      call chain.
	 * @param ids           ids of data items to be deleted.
	 * @throws ApplicationException when error occured.
	 */
	public void deleteByIds(String correlationId, K[] ids) throws ApplicationException {

		checkOpened(correlationId);

		Bson filter = Filters.in("_id", ids);

		DeleteResult result = _collection.deleteMany(filter);

		_logger.trace(correlationId, "Deleted %d from %s", result.getDeletedCount(), _collectionName);
	}

//	protected Bson composeFilter(FilterParams filterParams) {
//		filterParams = filterParams != null ? filterParams : new FilterParams();
//
//		ArrayList<Bson> filters = new ArrayList<Bson>();
//
//		String ids = filterParams.getAsNullableString("ids");
//		if (ids != null) {
//			String[] idTokens = ids.split(",");
//			if (idTokens.length > 0)
//				filters.add(Filters.in("_id", idTokens));
//		}
//
//		return Filters.and(filters);
//	}
//
//	protected Bson composeUpdate(AnyValueMap updateMap) {
//		updateMap = updateMap != null ? updateMap : new AnyValueMap();
//
//		List<Bson> updateDefinitions = new ArrayList<Bson>();
//
//		for (String key : updateMap.keySet()) {
//			updateDefinitions.add(Updates.set(key, updateMap.get(key)));
//		}
//
//		return Updates.combine(updateDefinitions);
//	}
//
//	protected Bson composeSort(SortParams sortParams) {
//		sortParams = sortParams != null ? sortParams : new SortParams();
//		BsonDocument sort = new BsonDocument();
//
//		for (SortField key : sortParams) {
//			sort.append(key.getName(), new BsonInt32(key.isAscending() ? 1 : -1));
//		}
//
//		return sort;
//	}

}
