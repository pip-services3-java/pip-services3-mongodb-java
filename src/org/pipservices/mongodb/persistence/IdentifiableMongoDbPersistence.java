package org.pipservices.mongodb.persistence;

import java.util.*;

import org.bson.*;
import org.bson.conversions.*;
import org.pipservices.commons.config.*;
import org.pipservices.commons.data.*;
import org.pipservices.commons.errors.*;

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.client.result.*;

public class IdentifiableMongoDbPersistence<T extends IIdentifiable<K>, K> extends MongoDbPersistence<T>
	/*implements IWriter<T, K>, IGetter<T, K>, ISetter<T>*/ {

	protected int _maxPageSize = 100;

	public IdentifiableMongoDbPersistence(String collectionName, Class<T> documentClass) {
		super(collectionName, documentClass);
	}

	public void configure(ConfigParams config) {
		super.configure(config);

		_maxPageSize = config.getAsIntegerWithDefault("options.max_page_size", _maxPageSize);
	}

	protected DataPage<T> getPageByFilter(String correlationId, Bson filter, PagingParams paging, Bson sort)
		throws ApplicationException {

		checkOpened(correlationId);
		
		filter = filter != null ? filter : new Document();
		FindIterable<T> query = _collection.find(filter);
		
		if (sort != null)
			query = query.sort(sort);
		
		paging = paging != null ? paging : new PagingParams();
		int skip = (int)paging.getSkip(0);
		int take = (int)paging.getTake(_maxPageSize);
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

	protected List<T> getListByFilter(String correlationId, Bson filter, Bson sort)
		throws ApplicationException {
		
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

	public List<T> getListByIds(String correlationId, K[] ids)
		throws ApplicationException {
		
		Bson filter = Filters.in("_id", ids);

		return getListByFilter(correlationId, filter, null);
	}

	public T getOneById(String correlationId, K id)
		throws ApplicationException {
		
		checkOpened(correlationId);
		
		Bson filter = Filters.eq("_id", id);
				
		T item = _collection.find(filter).first();

		if (item == null)
			_logger.trace(correlationId, "Nothing found from %s with id = %s", _collectionName, id.toString());
		else
			_logger.trace(correlationId, "Retrieved from %s with id = %s", _collectionName, id.toString());

		return item;
	}

	protected T getOneRandom(String correlationId, Bson filter)
		throws ApplicationException {
		
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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public T set(String correlationId, T newItem)
		throws ApplicationException {

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

	public T сreate(String correlationId, T item)
		throws ApplicationException {

		checkOpened(correlationId);
		
		if (item instanceof IStringIdentifiable && item.getId() == null)
			((IStringIdentifiable) item).setId(IdGenerator.nextLong());

		_collection.insertOne(item);

		_logger.trace(correlationId, "Created in %s with id = %s", _collectionName, item.getId().toString());

		return item;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public T update(String correlationId, T newItem)
		throws ApplicationException {

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

	public T updatePartially(String correlationId, K id, AnyValueMap data)
		throws ApplicationException {
		
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

	public T deleteById(String correlationId, K id)
		throws ApplicationException {
	
		checkOpened(correlationId);
		
		Bson filter = Filters.eq("_id", id);
		
		FindOneAndDeleteOptions options = new FindOneAndDeleteOptions();
		
		T result = _collection.findOneAndDelete(filter, options);

		_logger.trace(correlationId, "Deleted from %s with id = %s", _collectionName, id.toString());

		return result;
	}

	protected void deleteByFilter(String correlationId, Bson filter)
		throws ApplicationException {
	
		checkOpened(correlationId);
		
		filter = filter != null ? filter : new Document();
		DeleteResult result = _collection.deleteMany(filter);

		_logger.trace(correlationId, "Deleted %d from %s", result.getDeletedCount(), _collectionName);
	}

	public void deleteByIds(String correlationId, K[] ids)
		throws ApplicationException {
	
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
