package org.pipservices.mongodb;

import java.util.*;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.conversions.Bson;
import org.pipservices.commons.config.ConfigParams;
import org.pipservices.commons.data.AnyValueMap;
import org.pipservices.commons.data.DataPage;
import org.pipservices.commons.data.FilterParams;
import org.pipservices.commons.data.IIdentifiable;
import org.pipservices.commons.data.IStringIdentifiable;
import org.pipservices.commons.data.IdGenerator;
import org.pipservices.commons.data.PagingParams;
import org.pipservices.commons.data.SortField;
import org.pipservices.commons.data.SortParams;
import org.pipservices.data.IGetter;
import org.pipservices.data.ISetter;
import org.pipservices.data.IWriter;

import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.*;
import com.mongodb.client.model.ReturnDocument;

public class IdentifiableMongoDbPersistence<T extends IIdentifiable<K>, K> extends MongoDbPersistence<T>
		implements IWriter<T, K>, IGetter<T, K>, ISetter<T> {

	protected int _maxPageSize = 100;

	protected static final String internalIdFieldName = "_id";

	public IdentifiableMongoDbPersistence(String collectionName) {
		super(collectionName);
	}

	public void configure(ConfigParams config) {
		super.configure(config);

		_maxPageSize = config.getAsIntegerWithDefault("options.max_page_size", _maxPageSize);
	}

	public DataPage<T> getPageByFilter(String correlationId, Bson filterDefinition, 
			PagingParams paging, Bson sortDefinition) {
		
		//BsonDocument filter = filterDefinition.toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());//????
		//sort
		
		FindIterable<T> query = _collection.find(filterDefinition);
		if (sortDefinition != null)
			query = query.sort(sortDefinition);
		paging = paging != null ? paging : new PagingParams();
		long skip = paging.getSkip(0); 
		long take = paging.getTake(_maxPageSize);
		
		long count = paging.hasTotal() ? _collection.count(filterDefinition) : 0;
		
		List<T> items = new ArrayList<T>();				
		query.skip((int)skip).limit((int)take).forEach((Block<? super T>)block -> items.add(block));

		_logger.trace(correlationId, "Retrieved {items.Count} from {_collection}");

		
		return new DataPage<T>(items, count);
	}
	
	public List<T> getListByFilter(String correlationId, Bson filterDefinition, Bson sortDefinition) {
		
		//BsonDocument filter = filterDefinition.toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());//????
		//sort
		
		FindIterable<T> query = _collection.find(filterDefinition);
		if (sortDefinition != null)
			query = query.sort(sortDefinition);
		
		List<T> items = new ArrayList<T>();				
		query.forEach((Block<? super T>)block -> items.add(block));

		_logger.trace(correlationId, "Retrieved {items.Count} from {_collection}");

		
		return items;
	}
	
	public List<T> getListByIds(String correlationId, K[] ids){
		List<T> result = new ArrayList<T>();
    	for( K oneId : ids ) {
    		T item = getOneById(correlationId, oneId);
    		if ( item != null )
    			result.add(item);
    	}
    	_logger.trace(correlationId, "Retrieved {items.Count} from {_collection}");
    	
    	return result;
	}
	
	@Override
	public T getOneById(String correlationId, K id) {
		FindIterable<T> result = _collection.find( Filters.eq("id", id) );

        if (result == null)
        {
            _logger.trace(correlationId, "Nothing found from {0} with id = {1}", _collectionName, id);
            return null;
        }
        _logger.trace(correlationId, "Retrieved from {0} with id = {1}", _collectionName, id);

        return result.first();
	}
	
	public T getOneRandom(String correlationId, Bson filterDefinition) {		
		int count = (int)_collection.count(filterDefinition);
		if (count <= 0)
        {
            _logger.trace(correlationId, "Nothing found for filter {0}", filterDefinition.toString());
            return null;
        }

		int randomIndex = new Random().nextInt(count - 1);
		FindIterable<T> result = _collection.find(filterDefinition).skip(randomIndex);
		_logger.trace(correlationId, "Retrieved randomly from {0} with id = {1}", _collectionName, result.first().getId());
		
		return result.first();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public T set(String correlationId, T newItem) {

    	if (newItem instanceof IIdentifiable && newItem.getId() == null)
    		((IIdentifiable)newItem).setId(IdGenerator.nextLong());    	
	
    	Bson filterDefinition = Filters.eq("id", newItem.getId());
    	FindOneAndReplaceOptions options = new FindOneAndReplaceOptions();
    	options.returnDocument(ReturnDocument.AFTER);
    	options.upsert(true);
    	
    	T result = _collection.findOneAndReplace(filterDefinition, newItem, options);
    	
    	_logger.trace(correlationId, "Set in {0} with id = {1}", _collectionName, newItem.getId());
	        
	    return result;
	}

	@Override
	public T create(String correlationId, T item) {
    	if (item instanceof IStringIdentifiable && item.getId() == null)
    		((IStringIdentifiable)item).setId(IdGenerator.nextLong());
    	
    	synchronized (_lock) {
	    	_collection.insertOne(item);	
	    	
	    	_logger.trace(correlationId, "Created in {0} with id = {1}", _collectionName, item.getId());	    	
    	}
        return item;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public T update(String correlationId, T newItem) {

    	if (newItem instanceof IIdentifiable && newItem.getId() == null)
    		((IIdentifiable)newItem).setId(IdGenerator.nextLong());    	
	
    	Bson filterDefinition = Filters.eq("id", newItem.getId());
    	FindOneAndReplaceOptions options = new FindOneAndReplaceOptions();
    	options.returnDocument(ReturnDocument.AFTER);
    	options.upsert(false);
    	
    	T result = _collection.findOneAndReplace(filterDefinition, newItem, options);
    	
    	_logger.trace(correlationId, "Update in {0} with id = {1}", _collectionName, newItem.getId());
	        
	    return result;
	}
	
	public T modify(String correlationId, Bson filterDefinition, Bson newItem) {

    	if (filterDefinition == null && newItem == null)
    		return null;    	
	
    	FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
    	options.returnDocument(ReturnDocument.AFTER);
    	options.upsert(false);
    	
    	T result = _collection.findOneAndUpdate(filterDefinition, newItem, options);
    	
    	_logger.trace(correlationId, "Update in {0}", _collectionName);
	        
	    return result;
	}
	
	public T modifyById(String correlationId, K id, Bson newItem) {

    	if (id == null && newItem == null)
    		return null;    	
    	
    	T result = modify(correlationId, Filters.eq("id", id), newItem);
    	
    	_logger.trace(correlationId, "Modify in {0} with id = {1}", _collectionName, id);
	        
	    return result;
	}

	@Override
	public T deleteById(String correlationId, K id) {
		Bson filterDefinition = Filters.eq("id",id);
    	FindOneAndDeleteOptions options = new FindOneAndDeleteOptions();
        T result = _collection.findOneAndDelete(filterDefinition, options);

        _logger.trace(correlationId, "Deleted from {0} with id = {1}", _collectionName, id);

        return result;
	}
	
	public void deleteByFilter(String correlationId, Bson filterDefinition){
		_collection.deleteMany(filterDefinition);

        _logger.trace(correlationId, "Deleted {result.DeletedCount} from {_collection}");
	}
	
	public void deleteByIds(String correlationId, K[] ids) {
		Bson filterDefinition = Filters.in("id", ids);
		
		_collection.deleteMany(filterDefinition);

        _logger.trace(correlationId, "Deleted {result.DeletedCount} from {_collection}");
	}
	
	
	protected Bson composeFilter(FilterParams filterParams) {
        filterParams = filterParams != null ? filterParams : new FilterParams();
        BsonDocument filter = new BsonDocument();
        
        for (String filterKey : filterParams.keySet())
        {
            if (filterKey.equals("ids"))
            {
            	String idParam = filterParams.getAsNullableString("ids");
                filter.append( filterKey, isArray(idParam) ? toBsonArrayOfType(idParam) :
                	new BsonString(idParam) );
                continue;
            }

            String filterParam = filterParams.get(filterKey);

            filter.append(filterKey, isArray(filterParam) ? toBsonArrayOfType(filterParam) :
                new BsonString(filterParam) );
        }

        return filter;

    }
	
	protected Bson composeUpdate(AnyValueMap updateMap) {
        updateMap = updateMap != null ? updateMap : new AnyValueMap();

        List<Bson> updateDefinitions = new ArrayList<Bson>();

        for (String key : updateMap.keySet())
        {
            updateDefinitions.add(Updates.set(key, updateMap.get(key)));
        }

        return  Updates.combine(updateDefinitions);
    }
	
	protected Bson composeSort(SortParams sortParams) {
        sortParams = sortParams != null ? sortParams : new SortParams();
        BsonDocument sort = new BsonDocument();
        for(SortField key : sortParams) {
        	sort.append( key.getName(), new BsonInt32( key.isAscending() ? 1 : -1) );
        }
        return sort;
       
    }
	
	private static BsonArray toBsonArrayOfType( String value ) {
		BsonArray array = new BsonArray();
		for(String field : toArrayOfType(value) )
			array.add(new BsonString(field));
		return array;		
	}
	
	protected static String[] toArrayOfType(String value) {
        if (value == null)
        {
            return null;
        }

        String[] items = value.split( "," );
        return (items != null && items.length > 0) ? items : null;
    }
	
	protected static Boolean isArray(String value) {
        if (isNullOrWhiteSpace(value))
        {
            return false;
        }

        return value.split(",").length > 1;
    }
}
