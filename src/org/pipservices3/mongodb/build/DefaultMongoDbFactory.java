package org.pipservices3.mongodb.build;

import org.pipservices3.commons.refer.Descriptor;
import org.pipservices3.components.build.Factory;
import org.pipservices3.mongodb.connect.MongoDbConnection;

/**
 * Creates MongoDb components by their descriptors.
 *
 * @see Factory
 * @see org.pipservices3.mongodb.connect.MongoDbConnection
 */
public class DefaultMongoDbFactory extends Factory {
    private static final Descriptor MongoDbConnectionDescriptor = new Descriptor("pip-services", "connection", "mongodb", "*", "1.0");

    /**
     * Create a new instance of the factory.
     */
    public DefaultMongoDbFactory() {
        this.registerAsType(DefaultMongoDbFactory.MongoDbConnectionDescriptor, MongoDbConnection.class);
    }
}
