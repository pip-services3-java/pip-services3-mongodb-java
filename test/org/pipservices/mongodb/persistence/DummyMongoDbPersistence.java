package org.pipservices.mongodb.persistence;

import java.util.ArrayList;
import java.util.List;

import org.bson.*;
import org.bson.conversions.*;
import org.pipservices.commons.data.*;
import org.pipservices.commons.errors.*;
import org.pipservices.commons.run.*;

import com.mongodb.client.model.Filters;

public class DummyMongoDbPersistence extends IdentifiableMongoDbPersistence<Dummy, String>
	implements IDummyPersistence, ICleanable {

	public DummyMongoDbPersistence() {
		super("dummies", Dummy.class);
	}

    public DataPage<Dummy> getPageByFilter(String correlationId, FilterParams filter, PagingParams paging) throws ApplicationException {
    	filter = filter != null ? filter : new FilterParams();
    	
    	List<Bson> filters = new ArrayList<Bson>();

    	String key = filter.getAsNullableString("key");
    	if (key != null)
    		filters.add(Filters.eq("key", key));
    	
    	Bson filterDefinition = filters.size() > 0 ? Filters.and(filters) : null;
    	    	
		return super.getPageByFilter(correlationId, filterDefinition, paging, null);
	}
	
}
