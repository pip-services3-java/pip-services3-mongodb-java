package org.pipservices.fixtures;

import org.bson.conversions.Bson;
import org.pipservices.commons.data.AnyValueMap;
import org.pipservices.commons.data.DataPage;
import org.pipservices.commons.data.FilterParams;
import org.pipservices.commons.data.PagingParams;
import org.pipservices.commons.data.SortParams;
import org.pipservices.mongodb.IdentifiableMongoDbPersistence;

public class MongoDbDummyPersistence extends IdentifiableMongoDbPersistence<Dummy, String> implements IDummyPersistence{

	public MongoDbDummyPersistence() {
		super("dummies");
	}

	@Override
	public Dummy сreate(String correlationId, Dummy item) {
		return create(correlationId, item); 
	}

	@Override
	public void clear() {
		clear();		
	}

	public Dummy delete(String correlationId, String id) {
        return deleteById(correlationId, id);
    }

    public DataPage<Dummy> getByFilter(String correlationId, FilterParams filter, PagingParams paging, SortParams sort) {
        return getPageByFilter(correlationId, composeFilter(filter), paging, composeSort(sort));
    }

    public Dummy getById(String correlationId, String id) {
        return getOneById(correlationId, id);
    }

	@Override
	public Dummy modify(String correlationId, Bson filterDefinition, Bson item) {
		return modify(correlationId, filterDefinition, item);
	}

	@Override
	public Dummy modifyById(String correlationId, String id, Bson item) {
		return modifyById(correlationId, id, item);
	}

	@Override
	public Dummy modify(String correlationId, String id, AnyValueMap updateMap) {
		return modifyById(correlationId, id, composeUpdate(updateMap));
	}
}
