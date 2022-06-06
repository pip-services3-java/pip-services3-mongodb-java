package org.pipservices3.mongodb.persistence;

import com.mongodb.client.model.IndexOptions;
import org.bson.conversions.Bson;

/**
 * Index definition for mondodb
 */
public class MongoDbIndex {
    public MongoDbIndex() {

    }

    public MongoDbIndex(Bson keys, IndexOptions options) {
        this.keys = keys;
        this.options = options;
    }

    /**
     * Index keys (fields)
     */
    public Bson keys;
    /**
     * Index options
     */
    public IndexOptions options;
}
