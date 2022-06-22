package org.pipservices3.mongodb.persistence;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.mongodb.client.model.IndexOptions;
import org.bson.Document;
import org.bson.conversions.*;
import org.pipservices3.commons.data.*;
import org.pipservices3.commons.run.*;

import com.mongodb.client.model.Filters;
import org.pipservices3.mongodb.fixtures.Dummy;
import org.pipservices3.mongodb.fixtures.IDummyPersistence;

import javax.print.Doc;

public class DummyMongoDbPersistence extends IdentifiableMongoDbPersistence<Dummy, String>
        implements IDummyPersistence, ICleanable {

    public DummyMongoDbPersistence() {
        super("dummies", Dummy.class);
    }

    @Override
    protected void defineSchema() {
        this.ensureIndex(new Document("key", 1), new IndexOptions());
    }

    public DataPage<Dummy> getPageByFilter(String correlationId, FilterParams filter, PagingParams paging) {
        filter = filter != null ? filter : new FilterParams();

        List<Bson> filters = new ArrayList<Bson>();

        String key = filter.getAsNullableString("key");
        if (key != null)
            filters.add(Filters.eq("key", key));

        String keys = filter.getAsNullableString("keys");
        if (keys != null)
            filters.add(Filters.in("key", Arrays.stream(keys.split(",")).toList()));

        Bson filterDefinition = filters.size() > 0 ? Filters.and(filters) : null;

        return super.getPageByFilter(correlationId, filterDefinition, paging, null, null);
    }

    @Override
    public long getCountByFilter(String correlationId, FilterParams filter) {
        filter = filter != null ? filter : new FilterParams();
        var key = filter.getAsNullableString("key");

        var filterCondition = new Document();

        if (key != null)
            filterCondition.put("key", key);

        return super.getCountByFilter(correlationId, filterCondition);
    }
}
