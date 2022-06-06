package org.pipservices3.mongodb.fixtures;

import static org.junit.Assert.*;

import java.util.*;

import org.pipservices3.commons.data.*;
import org.pipservices3.commons.errors.*;

public class DummyPersistenceFixture {

    private final Dummy _dummy1 = new Dummy("1", "Key 1", "Content 1");
    private final Dummy _dummy2 = new Dummy("2", "Key 2", "Content 2");

    private final IDummyPersistence _persistence;

    public DummyPersistenceFixture(IDummyPersistence persistence) {
        assertNotNull(persistence);

        _persistence = persistence;
    }

    public void testCrudOperations() throws ApplicationException {
        // Create one dummy
        Dummy dummy1 = _persistence.create(null, _dummy1);

        assertNotNull(dummy1);
        assertNotNull(dummy1.getId());
        assertEquals(_dummy1.getKey(), dummy1.getKey());
        assertEquals(_dummy1.getContent(), dummy1.getContent());

        // Create another dummy
        Dummy dummy2 = _persistence.create(null, _dummy2);

        assertNotNull(dummy2);
        assertNotNull(dummy2.getId());
        assertEquals(_dummy2.getKey(), dummy2.getKey());
        assertEquals(_dummy2.getContent(), dummy2.getContent());

        // Get page by filter
        DataPage<Dummy> page = _persistence.getPageByFilter(null, null, null);
        assertNotNull(page);
        assertEquals(2, page.getData().size());

        // Update the dummy
        dummy1.setContent("Updated Content 1");
        Dummy dummy = _persistence.update(null, dummy1);

        assertNotNull(dummy);
        assertEquals(dummy1.getId(), dummy.getId());
        assertEquals(_dummy1.getKey(), dummy.getKey());
        assertEquals(_dummy1.getContent(), dummy.getContent());

        // Partially update the dummy
        dummy = _persistence.updatePartially(
                null, dummy1.getId(), AnyValueMap.fromTuples("content", "Partially updated content 1"));

        assertNotNull(dummy);
        assertEquals(dummy1.getId(), dummy.getId());
        assertEquals(_dummy1.getKey(), dummy.getKey());
        assertEquals("Partially updated content 1", dummy.getContent());

        // Get the dummy by Id
        dummy = _persistence.getOneById(null, dummy.getId());

        assertNotNull(dummy);
        assertEquals(dummy1.getId(), dummy.getId());
        assertEquals(_dummy1.getKey(), dummy.getKey());
        assertEquals("Partially updated content 1", dummy.getContent());

        // Delete the dummy
        _persistence.deleteById(null, dummy1.getId());

        // Try to get deleted dummy
        dummy = _persistence.getOneById(null, dummy1.getId());
        assertNull(dummy);

        var count = this._persistence.getCountByFilter(null, null);
        assertEquals(count, 1);
    }

    public void testBatchOperations() throws ApplicationException {
        // Create one dummy
        Dummy dummy1 = _persistence.create(null, _dummy1);

        assertNotNull(dummy1);
        assertNotNull(dummy1.getId());
        assertEquals(_dummy1.getKey(), dummy1.getKey());
        assertEquals(_dummy1.getContent(), dummy1.getContent());

        // Create another dummy
        Dummy dummy2 = _persistence.create("", _dummy2);

        assertNotNull(dummy2);
        assertNotNull(dummy2.getId());
        assertEquals(_dummy2.getKey(), dummy2.getKey());
        assertEquals(_dummy2.getContent(), dummy2.getContent());

        // Read batch
        List<Dummy> dummies = _persistence.getListByIds(null, new String[]{dummy1.getId(), dummy2.getId()});

        assertEquals(2, dummies.size());

        // Delete batch
        _persistence.deleteByIds(null, new String[]{dummy1.getId(), dummy2.getId()});

        // Read empty batch
        dummies = _persistence.getListByIds(null, new String[]{dummy1.getId(), dummy2.getId()});
        assertEquals(0, dummies.size());
    }

}
