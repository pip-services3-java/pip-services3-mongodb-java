package org.pipservices.mongodb.persistence;

import java.util.*;

import org.bson.*;
import org.bson.conversions.*;
import org.pipservices.commons.config.*;
import org.pipservices.commons.data.*;
import org.pipservices.data.*;

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.client.result.*;

public class IdentifiableMongoDbPersistence<T extends IIdentifiable<K>, K> extends MongoDbPersistence<T>
		implements IWriter<T, K>, IGetter<T, K>, ISetter<T> {

	protected int _maxPageSize = 100;

	protected static final String internalIdFieldName = "_id";

	public IdentifiableMongoDbPersistence(String collectionName, Class<T> documentClass) {
		super(collectionName, documentClass);
	}

	public void configure(ConfigParams config) {
		super.configure(config);

		_maxPageSize = config.getAsIntegerWithDefault("options.max_page_size", _maxPageSize);
	}

	protected DataPage<T> getPageByFilter(String correlationId, FilterParams filterDefinition, 
			PagingParams paging, SortParams sortDefinition) {
		
		FindIterable<T> query = _collection.find(composeFilter(filterDefinition));
		if (sortDefinition != null)
			query = query.sort(composeSort(sortDefinition));
		paging = paging != null ? paging : new PagingParams();
		long skip = paging.getSkip(0); 
		long take = paging.getTake(_maxPageSize);
		
		long count = paging.hasTotal() ? _collection.count(composeFilter(filterDefinition)) : 0;
		
		List<T> items = new ArrayList<T>();				
		query.skip((int)skip).limit((int)take).forEach((Block<T>)block -> items.add(block));
//		for( T item : query.skip((int)skip).limit((int)take) ) {
//			items.add(item);
//		}
//		MongoCursor<T> cursor = query.skip((int)skip).limit((int)take).iterator();
//	    try {
//	        while (cursor.hasNext()) {
//	            T document = cursor.next();
//	            items.add(document);
//	        }
//	    } finally {
//	        cursor.close();
//	    }

		_logger.trace(correlationId, "Retrieved %d from %s", items.size(), _collectionName);

		if( count == 0 )
			return new DataPage<T>(items);
		return new DataPage<T>(items, count);
	}
	
	protected List<T> getListByFilter(String correlationId, FilterParams filterDefinition, SortParams sortDefinition) {
		FindIterable<T> query = _collection.find(composeFilter(filterDefinition));
		if (sortDefinition != null)
			query = query.sort((composeSort(sortDefinition)));
		
		List<T> items = new ArrayList<T>();				
		query.forEach((Block<? super T>)block -> items.add(block));

		_logger.trace(correlationId, "Retrieved %d from %s", items.size(), _collectionName);

		
		return items;
	}
	
	public List<T> getListByIds(String correlationId, K[] ids){
		List<T> items = new ArrayList<T>();
    	
		for(K oneId : ids) {
    		T item = getOneById(correlationId, oneId);
    		if (item != null)
    			items.add(item);
    	}
    	
    	_logger.trace(correlationId, "Retrieved %d from %s", items.size(), _collectionName);
    	
    	return items;
	}
	
	@Override
	public T getOneById(String correlationId, K id) {
		FindIterable<T> result = _collection.find(Filters.eq("_id", id) );

        if (result == null) {
            _logger.trace(correlationId, "Nothing found from %s with id = %s", _collectionName, id.toString());
            return null;
        }
        
        _logger.trace(correlationId, "Retrieved from %s with id = %s", _collectionName, id.toString());

        return result.first();
	}
	
	public T getOneRandom(String correlationId, FilterParams filterDefinition) {		
		int count = (int)_collection.count(composeFilter(filterDefinition));

		if (count <= 0) {
            _logger.trace(correlationId, "Nothing found for filter %s", filterDefinition.toString());
            return null;
        }

		int randomIndex = new Random().nextInt(count - 1);
		FindIterable<T> result = _collection.find(composeFilter(filterDefinition)).skip(randomIndex);
		_logger.trace(correlationId, "Retrieved randomly from %s with id = %s", _collectionName, result.first().getId().toString());
		
		return result.first();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public T set(String correlationId, T newItem) {
    	if (newItem instanceof IIdentifiable && newItem.getId() == null)
    		((IIdentifiable)newItem).setId(IdGenerator.nextLong());    	
	
    	Bson filterDefinition = Filters.eq("_id", newItem.getId());
    	FindOneAndReplaceOptions options = new FindOneAndReplaceOptions();
    	options.returnDocument(ReturnDocument.AFTER);
    	options.upsert(true);
    	
    	T result = _collection.findOneAndReplace(filterDefinition, newItem, options);
    	
    	_logger.trace(correlationId, "Set in %s with id = %s", _collectionName, newItem.getId().toString());
	        
	    return result;
	}

	@Override
	public T create(String correlationId, T item) {
    	if (item instanceof IStringIdentifiable && item.getId() == null)
    		((IStringIdentifiable)item).setId(IdGenerator.nextLong());
    	
    	_collection.insertOne(item);	
    	
    	_logger.trace(correlationId, "Created in %s with id = %s", _collectionName, item.getId().toString());	    	

    	return item;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public T update(String correlationId, T newItem) {
    	if (newItem instanceof IIdentifiable && newItem.getId() == null)
    		((IIdentifiable)newItem).setId(IdGenerator.nextLong());    	
	
    	Bson filterDefinition = Filters.eq("_id", newItem.getId());
    	FindOneAndReplaceOptions options = new FindOneAndReplaceOptions();
    	options.returnDocument(ReturnDocument.AFTER);
    	options.upsert(false);
    	
    	T result = _collection.findOneAndReplace(filterDefinition, newItem, options);
    	
    	_logger.trace(correlationId, "Update in %s with id = %s", _collectionName, newItem.getId().toString());
	        
	    return result;
	}
	
	//Todo 
//	protected T modify(String correlationId, Bson filterDefinition, Bson newItem) {
//    	if (filterDefinition == null && newItem == null)
//    		return null;    	
//	
//    	FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
//    	options.returnDocument(ReturnDocument.AFTER);
//    	options.upsert(false);
//    	
//    	T result = _collection.findOneAndUpdate(filterDefinition, newItem, options);
//    	
//    	_logger.trace(correlationId, "Update in %s", _collectionName);
//	        
//	    return result;
//	}
	
	protected T modify(String correlationId, K id, AnyValueMap updateMap) {
    	if (id == null && updateMap == null)
    		return null;    	
	
    	FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
    	options.returnDocument(ReturnDocument.AFTER);
    	options.upsert(false);
    	
    	T result = _collection.findOneAndUpdate(Filters.eq("_id", id), composeUpdate(updateMap), options);
    	
    	_logger.trace(correlationId, "Update in %s", _collectionName);
	        
	    return result;
	}
	
	
	// Todo
//	public T modifyById(String correlationId, K id, Bson newItem) {
//    	if (id == null && newItem == null)
//    		return null;    	
//    	
//    	T result = modify(correlationId, Filters.eq("_id", id), newItem);
//    	
//    	_logger.trace(correlationId, "Modify in %s with id = %s", _collectionName, id.toString());
//	        
//	    return result;
//	}

	@Override
	public T deleteById(String correlationId, K id) {
		Bson filterDefinition = Filters.eq("_id",id);
    	FindOneAndDeleteOptions options = new FindOneAndDeleteOptions();
        T result = _collection.findOneAndDelete(filterDefinition, options);

        _logger.trace(correlationId, "Deleted from %s with id = %s", _collectionName, id.toString());

        return result;
	}
	
	protected void deleteByFilter(String correlationId, FilterParams filterDefinition){
		DeleteResult result = _collection.deleteMany(composeFilter(filterDefinition));

        _logger.trace(correlationId, "Deleted %d from %s", result.getDeletedCount(), _collectionName);
	}
	
	public void deleteByIds(String correlationId, K[] ids) {
		Bson filterDefinition = Filters.in("_id", ids);
		
		DeleteResult result = _collection.deleteMany(filterDefinition);

        _logger.trace(correlationId, "Deleted %d from %s", result.getDeletedCount(), _collectionName);
	}
	
	
	protected Bson composeFilter(FilterParams filterParams) {
        filterParams = filterParams != null ? filterParams : new FilterParams();

        ArrayList<Bson> filters = new ArrayList<Bson>();
        
		String ids = filterParams.getAsNullableString("ids");
        if (ids != null) {
        	String[] idTokens = ids.split(",");
        	if (idTokens.length > 0)
        		filters.add(Filters.in("_id", idTokens));
        }

        return Filters.and(filters);
    }
	
	protected Bson composeUpdate(AnyValueMap updateMap) {
        updateMap = updateMap != null ? updateMap : new AnyValueMap();

        List<Bson> updateDefinitions = new ArrayList<Bson>();

        for (String key : updateMap.keySet()) {
            updateDefinitions.add(Updates.set(key, updateMap.get(key)));
        }

        return Updates.combine(updateDefinitions);
    }
	
	protected Bson composeSort(SortParams sortParams) {
        sortParams = sortParams != null ? sortParams : new SortParams();
        BsonDocument sort = new BsonDocument();
        
        for(SortField key : sortParams) {
        	sort.append(key.getName(), new BsonInt32(key.isAscending() ? 1 : -1));
        }

        return sort;       
    }
	
//	private static BsonArray toBsonArrayOfType(String value) {
//		BsonArray array = new BsonArray();
//		for(String field : toArrayOfType(value) )
//			array.add(new BsonString(field));
//		return array;		
//	}
	
//	protected static String[] toArrayOfType(String value) {
//        if (value == null)
//            return null;
//
//        String[] items = value.split( "," );
//        return (items != null && items.length > 0) ? items : null;
//    }
//	
//	protected static Boolean isArray(String value) {
//        if (value == null)
//            return false;
//
//        return value.split(",").length > 1;
//    }
}
