package org.pipservices.fixtures;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.time.ZonedDateTime;
import java.util.*;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.conversions.Bson;
import org.pipservices.commons.data.AnyValueMap;
import org.pipservices.commons.data.DataPage;
import org.pipservices.commons.data.FilterParams;
import org.pipservices.commons.data.SortField;
import org.pipservices.commons.data.SortParams;
import org.pipservices.mongodb.MongoDbPersistence;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

public class PersistenceFixture {
	
	private final Dummy _dummy1 = new Dummy("1", "Key 1", "Content 1", ZonedDateTime.now(), 
			new InnerDummy("1", "Inner dummy name 1", "Inner dummy description 1"), new ArrayList<InnerDummy>());
	private final Dummy _dummy2 = new Dummy("2", "Key 2", "Content 2", ZonedDateTime.now(), 
			new InnerDummy("5", "Inner dummy name 5", "Inner dummy description 5"), new ArrayList<InnerDummy>());
	
	private final IDummyPersistence _persistence;
	
	 /**
	 * @param persistence
	 */
	public PersistenceFixture(IDummyPersistence persistence) {
         assertNotNull(persistence);

         _persistence = persistence;
         
         InnerDummy innerDummy_2 = new InnerDummy("2", "Inner dummy name 2", "Inner dummy description 2");
         InnerDummy innerDummy_3 = new InnerDummy("3", "Inner dummy name 3", "Inner dummy description 3");
         InnerDummy innerDummy_4 = new InnerDummy("4", "Inner dummy name 4", "Inner dummy description 4");
         InnerDummy innerDummy_100 = new InnerDummy("100", "Inner dummy name 100", "Inner dummy description 100");
         List<InnerDummy> innerDummies = new ArrayList<InnerDummy>();
         innerDummies.add(innerDummy_2);
         innerDummies.add(innerDummy_3);
         innerDummies.add(innerDummy_4);
         _dummy1.setInnerDummies(innerDummies);
         innerDummies.add(innerDummy_100);
         _dummy2.setInnerDummies(innerDummies);
         
     }
	 
	 
	public void testCrudOperations() {
        // Create one dummy
        Dummy dummy1 = _persistence.сreate(null, _dummy1);

        assertNotNull(dummy1);
        assertNotNull(dummy1.getId());
        assertEquals(_dummy1.getKey(), dummy1.getKey());
        assertEquals(_dummy1.getContent(), dummy1.getContent());

        // Create another dummy
        Dummy dummy2 = _persistence.сreate("", _dummy2);

        assertNotNull(dummy2);
        assertNotNull(dummy2.getId());
        assertEquals(_dummy2.getKey(), dummy2.getKey());
        assertEquals(_dummy2.getContent(), dummy2.getContent());

        // Update the dummy
        dummy1.setContent("Updated Content 1");
        Dummy dummy = _persistence.update(null, dummy1);

        assertNotNull(dummy);
        assertEquals(dummy1.getId(), dummy.getId());
        assertEquals(_dummy1.getKey(), dummy.getKey());
        assertEquals(_dummy1.getContent(), dummy.getContent());

        // Delete the dummy
        _persistence.deleteById(null, dummy1.getId());

        // Try to get deleted dummy
        dummy = _persistence.getOneById(null, dummy1.getId());
        assertNull(dummy);
    }
	
	public void testGetById() {
        // arrange
        Dummy dummy = _persistence.сreate(null, _dummy1);
        
        // act
        Dummy result = _persistence.getOneById(null, dummy.getId());

        // assert
        assertNotNull(dummy);
        assertEquals(dummy.getKey(), result.getKey());
        assertEquals(dummy.getContent(), result.getContent());
        assertEquals(dummy.getInnerDummy().getDescription(), result.getInnerDummy().getDescription());
        assertEquals(dummy.getCreateTimeUtc().toString(), result.getCreateTimeUtc().toString());
    }
	
	public void testGetByIdFromArray() {
        // arrange
        Dummy dummy = _persistence.сreate(null, _dummy1);

        // act
        Dummy result = _persistence.getOneById(null, dummy.getId());

        // assert
        assertNotNull(dummy);
        assertEquals(dummy.getKey(), result.getKey());
        assertEquals(dummy.getInnerDummies().get(0).getName(), result.getInnerDummies().get(0).getName());
        assertEquals(dummy.getInnerDummies().get(1).getDescription(), result.getInnerDummies().get(1).getDescription());
    }
	
	public void testGetPageByFilter() {
        // arrange 
        Dummy dummy1 = _persistence.сreate(null, _dummy1);
        Dummy dummy2 = _persistence.сreate(null, _dummy2);
        
        Bson filter = Filters.text("");

        // act
        DataPage<Dummy> result = _persistence.getPageByFilter(null, filter, null, null);

        // assert
        assertNotNull(result);
        assertEquals(2, result.getData().size());
        assertEquals(dummy1.getContent(), result.getData().get(0).getContent());
        assertEquals(dummy2.getInnerDummy().getDescription(), result.getData().get(1).getInnerDummy().getDescription());
    }
	
	public void testModifyExistingPropertiesBySelectedFields() {
        // arrange 
        Dummy dummy = _persistence.сreate(null, _dummy1);

        AnyValueMap updateMap = new AnyValueMap();
        updateMap.put("Content", "Modified Content");
        updateMap.put("InnerDummy.Description", "Modified InnerDummy Description");
        // act
        Dummy result = _persistence.modifyById(null, dummy.getId(), composeUpdate(updateMap));

        // assert
        assertNotNull(result);
        assertEquals(dummy.getId(), result.getId());
        assertEquals("Modified Content", result.getContent());
        assertEquals("Modified InnerDummy Description", result.getInnerDummy().getDescription());
    }
	
	public void testModifyNullPropertiesBySelectedFields() {
		Dummy dummy = _persistence.сreate(null, _dummy2);

        AnyValueMap updateMap = new AnyValueMap();
        updateMap.put("Content", "Modified Content");
        updateMap.put("InnerDummy", new InnerDummy("", "", "Modified InnerDummy Description"));

        // act
        Dummy result = _persistence.modifyById(null, dummy.getId(), composeUpdate(updateMap));

        // assert
        assertNotNull(result);
        assertEquals(dummy.getId(), result.getId());
        assertNotNull(dummy);
        assertEquals("Modified Content", result.getContent());
        assertEquals("Modified InnerDummy Description", result.getInnerDummy().getDescription());
    }
	
	public void testModifyNestedCollection() {
        // arrange 
		Dummy dummy = _persistence.сreate(null, _dummy1);

        List<InnerDummy> innerInnerDummies = new ArrayList<InnerDummy>();
        InnerDummy testInner1 = new InnerDummy("Test Inner Id #1", "Test Inner Dummy #1", ""), 
        		   testInner2 = new InnerDummy("Test Inner Id #2", "Test Inner Dummy #2", "");
        innerInnerDummies.add(testInner1);
        innerInnerDummies.add(testInner2);
        
        AnyValueMap updateMap = new AnyValueMap();
        updateMap.put( "inner_dummy.inner_inner_dummies", innerInnerDummies);

        // act - It's important to pass the type of updated object!
        Dummy result = _persistence.modifyById(null, dummy.getId(), composeUpdate(updateMap));

        // assert 1
        assertNotNull(result);
        assertEquals(innerInnerDummies.get(0).getId(), result.getInnerDummy().getInnerInnerDummies().get(0).getId());
        assertEquals(innerInnerDummies.get(0).getName(), result.getInnerDummy().getInnerInnerDummies().get(0).getName());
        assertEquals(innerInnerDummies.get(1).getId(), result.getInnerDummy().getInnerInnerDummies().get(1).getId());
        assertEquals(innerInnerDummies.get(1).getName(), result.getInnerDummy().getInnerInnerDummies().get(1).getName());
    }
	
	public void testSearchWithinNestedCollectionByFilter() {
        // arrange 
        Dummy dummy1 = _persistence.сreate(null, _dummy1);
        Dummy dummy2 = _persistence.сreate(null, _dummy2);

        FilterParams filterParams = extractFilterParams("inner_dummies.name:Inner dummy name 2");

        // act
        DataPage<Dummy> result = _persistence.getPageByFilter(null, composeFilter(filterParams), null, null);

        // assert
        assertNotNull(result);
        assertEquals(1, result.getData().size());
        assertEquals(dummy1.getId(), result.getData().get(0).getId());
    }
	
	public void testSearchWithinDeepNestedCollectionByFilter() {
		// arrange 
        Dummy dummy1 = _persistence.сreate(null, _dummy1);
        Dummy dummy2 = _persistence.сreate(null, _dummy2);

        FilterParams filterParams = extractFilterParams("inner_dummies.inner_inner_dummies.name:InnerInner Dummy#1");

        // act
        DataPage<Dummy> result = _persistence.getPageByFilter(null, composeFilter(filterParams), null, null);

        // assert
        assertNotNull(result);
        assertEquals(1, result.getData().size());
        assertEquals(dummy1.getId(), result.getData().get(0).getId());
    }
	
	public void testGetPageByIdsFilter(){
        // arrange 
		Dummy dummy1 = _persistence.сreate(null, _dummy1);
        Dummy dummy2 = _persistence.сreate(null, _dummy2);

        FilterParams filter = FilterParams.fromTuples(
            "ids", "1234567890,{dummy1.getId()}"
        );

        // act
        List<Dummy> result = _persistence.getListByFilter(null, composeFilter(filter), null);

        // assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(dummy1.getKey(), result.get(0).getKey());
        assertEquals(dummy2.getKey(), result.get(1).getKey());
    }
	
	public void testGetPageSortedByOneField() {
        // arrange 
		Dummy dummy1 = _persistence.сreate(null, _dummy1);
        Dummy dummy2 = _persistence.сreate(null, _dummy2);

        // keys: Key 1, Key 2, Key 3

        SortParams sortParams = new SortParams();
        sortParams.add(new SortField("key", false));

        // result -> 3 (Key 3), 2 (Key 2), 1 (Key 1)

        // act
        DataPage<Dummy> result = _persistence.getPageByFilter(null, null, null, composeSort(sortParams));

        // assert
        assertNotNull(result);
        assertEquals(2, result.getData().size());

        assertEquals(dummy2.getKey(), result.getData().get(1).getKey());
        assertEquals(dummy1.getKey(), result.getData().get(0).getKey());
    }
	
	private Bson composeUpdate(AnyValueMap updateMap) {
        updateMap = updateMap != null ? updateMap : new AnyValueMap();

        List<Bson> updateDefinitions = new ArrayList<Bson>();

        for (String key : updateMap.keySet())
        {
            updateDefinitions.add(Updates.set(key, updateMap.get(key)));
        }

        return  Updates.combine(updateDefinitions);
    }
	
	private Bson composeFilter(FilterParams filterParams) {
        filterParams = filterParams != null ? filterParams : new FilterParams();

        BsonDocument filter = new BsonDocument();
        for (String filterKey : filterParams.keySet()) {
        	String filterParam = filterParams.get(filterKey);

            filter.append(filterKey, isArray(filterParam) ? toBsonArrayOfType(filterParam) :
                new BsonString(filterParam) );
        }

        return filter;
    }
	
	private Bson composeSort(SortParams sortParams) {
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
	
	private static String[] toArrayOfType(String value) {
        if (value == null)
        {
            return null;
        }

        String[] items = value.split( "," );
        return (items != null && items.length > 0) ? items : null;
    }
	
	private static Boolean isArray(String value) {
        if (MongoDbPersistence.isNullOrWhiteSpace(value))
        {
            return false;
        }

        return value.split(",").length > 1;
    }
	
	private FilterParams extractFilterParams(String query) {
        FilterParams filterParams = new FilterParams();

        for (String filterParameter : query.split(",")) {
            String[] keyValue = filterParameter.split(":");

            if (keyValue.length == 2)
            {
                filterParams.put(keyValue[0], keyValue[1]);
            }
        }

        return filterParams;
    }
}
