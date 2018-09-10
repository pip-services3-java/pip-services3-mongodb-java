package org.pipservices.mongodb.persistence;

import java.util.List;

import org.bson.conversions.Bson;
import org.pipservices.commons.data.AnyValueMap;
import org.pipservices.commons.data.DataPage;
import org.pipservices.commons.data.FilterParams;
import org.pipservices.commons.data.PagingParams;
import org.pipservices.commons.data.SortParams;

public interface IDummyPersistence {

    Dummy сreate(String correlationId, Dummy item);
    void deleteByFilter(String correlationId, FilterParams filterDefinition);
    Dummy deleteById(String correlationId, String id);
    void deleteByIds(String correlationId, String[] ids);
    List<Dummy> getListByFilter(String correlationId, FilterParams filterDefinition, SortParams sortDefinition);
    List<Dummy> getListByIds(String correlationId, String[] ids);
    Dummy getOneById(String correlationId, String id);
    Dummy getOneRandom(String correlationId, FilterParams filterDefinition);    
    DataPage<Dummy> getPageByFilter(String correlationId, FilterParams filterDefinition, PagingParams paging, SortParams sortDefinition);

    Dummy modify(String correlationId, String id, AnyValueMap updateMap);
    Dummy set(String correlationId, Dummy item);
    Dummy update(String correlationId, Dummy item);
	void clear();
}
