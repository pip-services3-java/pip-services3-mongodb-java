package org.pipservices.fixtures;

import java.util.List;

import org.bson.conversions.Bson;
import org.pipservices.commons.data.AnyValueMap;
import org.pipservices.commons.data.DataPage;
import org.pipservices.commons.data.PagingParams;

public interface IDummyPersistence {

    Dummy сreate(String correlationId, Dummy item);
    void deleteByFilter(String correlationId, Bson filterDefinition);
    Dummy deleteById(String correlationId, String id);
    void deleteByIds(String correlationId, String[] ids);
    List<Dummy> getListByFilter(String correlationId, Bson filterDefinition, Bson sortDefinition);
    List<Dummy> getListByIds(String correlationId, String[] ids);
    Dummy getOneById(String correlationId, String id);
    Dummy getOneRandom(String correlationId, Bson filterDefinition);    
    DataPage<Dummy> getPageByFilter(String correlationId, Bson filterDefinition, PagingParams paging, Bson sortDefinition);
    
    Dummy modify(String correlationId, Bson filterDefinition, Bson item);
    Dummy modifyById(String correlationId, String id, Bson item);
    Dummy modify(String correlationId, String id, AnyValueMap updateMap);
    Dummy set(String correlationId, Dummy item);
    Dummy update(String correlationId, Dummy item);
	void clear();
}
