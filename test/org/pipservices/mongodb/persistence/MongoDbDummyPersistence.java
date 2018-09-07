package org.pipservices.mongodb.persistence;

import java.util.List;

import org.bson.conversions.Bson;
import org.pipservices.commons.data.AnyValueMap;
import org.pipservices.commons.data.DataPage;
import org.pipservices.commons.data.FilterParams;
import org.pipservices.commons.data.PagingParams;
import org.pipservices.commons.data.SortParams;
import org.pipservices.mongodb.persistence.IdentifiableMongoDbPersistence;

public class MongoDbDummyPersistence extends IdentifiableMongoDbPersistence<Dummy, String>
	implements IDummyPersistence{

	public MongoDbDummyPersistence() {
		super("dummies", Dummy.class);
	}

	@Override
	public Dummy сreate(String correlationId, Dummy item) {
		return super.create(correlationId, item); 
	}

	@Override
	public void clear() {
		super.clear(null);		
	}

	public Dummy deleteById(String correlationId, String id) {
        return super.deleteById(correlationId, id);
    }
	
    public void deleteByFilter(String correlationId, FilterParams filterDefinition) {
    	super.deleteByFilter(correlationId, filterDefinition);
	}
    
    public void deleteByIds(String correlationId, String[] ids) {
    	super.deleteByIds(correlationId, ids);
    }
    
    public List<Dummy> getListByFilter(String correlationId, FilterParams filterDefinition, SortParams sortDefinition) {
		return super.getListByFilter(correlationId, filterDefinition, sortDefinition);
	}
    public List<Dummy> getListByIds(String correlationId, String[] ids) {
		return super.getListByIds(correlationId, ids);
	}
    public Dummy getOneRandom(String correlationId, FilterParams filterDefinition) {
		return super.getOneRandom(correlationId, filterDefinition);
	}
    public Dummy set(String correlationId, Dummy item) {
		return super.set(correlationId, item);
	}
    public Dummy update(String correlationId, Dummy item) {
		return super.update(correlationId, item);
	}

    
    public DataPage<Dummy> getPageByFilter(String correlationId, FilterParams filter, PagingParams paging, SortParams sort) {
        return super.getPageByFilter(correlationId, filter, paging, sort);
    }

    public Dummy getOneById(String correlationId, String id) {
        return super.getOneById(correlationId, id);
    }

//	@Override
//	public Dummy modify(String correlationId, Bson filterDefinition, Bson item) {
//		return modify(correlationId, filterDefinition, item);
//	}
//
//	@Override
//	public Dummy modifyById(String correlationId, String id, Bson item) {
//		return modifyById(correlationId, id, item);
//	}

	@Override
	public Dummy modify(String correlationId, String id, AnyValueMap updateMap) {
		return super.modify(correlationId, id, updateMap);
	}
}
