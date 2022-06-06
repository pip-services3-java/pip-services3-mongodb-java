package org.pipservices3.mongodb.fixtures;

import java.util.*;

import org.pipservices3.commons.data.*;
import org.pipservices3.commons.errors.*;

public interface IDummyPersistence {
    DataPage<Dummy> getPageByFilter(String correlationId, FilterParams filterDefinition, PagingParams paging) throws ApplicationException;
    List<Dummy> getListByIds(String correlationId, String[] ids) throws ApplicationException;
    long getCountByFilter(String correlationId, FilterParams filter);
    Dummy getOneById(String correlationId, String id) throws ApplicationException;
	Dummy create(String correlationId, Dummy item);
    Dummy update(String correlationId, Dummy item) throws ApplicationException;
    Dummy updatePartially(String correlationId, String id, AnyValueMap update) throws ApplicationException;
    Dummy deleteById(String correlationId, String id) throws ApplicationException;
    void deleteByIds(String correlationId, String[] ids) throws ApplicationException;
}
