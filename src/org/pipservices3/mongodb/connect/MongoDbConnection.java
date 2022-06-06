package org.pipservices3.mongodb.connect;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.pipservices3.commons.config.ConfigParams;
import org.pipservices3.commons.config.IConfigurable;
import org.pipservices3.commons.errors.ApplicationException;
import org.pipservices3.commons.errors.ConfigException;
import org.pipservices3.commons.errors.ConnectionException;
import org.pipservices3.commons.refer.IReferenceable;
import org.pipservices3.commons.refer.IReferences;
import org.pipservices3.commons.refer.ReferenceException;
import org.pipservices3.commons.run.IOpenable;
import org.pipservices3.components.log.CompositeLogger;
import org.pipservices3.mongodb.codecs.DurationInt64Codec;
import org.pipservices3.mongodb.codecs.LocalDateStringCodec;
import org.pipservices3.mongodb.codecs.LocalDateTimeStringCodec;
import org.pipservices3.mongodb.codecs.ZonedDateTimeStringCodec;

import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * MongoDB connection using plain driver.
 * <p>
 * By defining a connection and sharing it through multiple persistence components
 * you can reduce number of used database connections.
 * <p>
 * ### Configuration parameters ###
 * <p>
 * <ul>
 * connection(s):
 * <li>discovery_key:             (optional) a key to retrieve the connection from {@link org.pipservices3.components.connect.IDiscovery}
 * <li>host:                      host name or IP address
 * <li>port:                      port number (default: 27017)
 * <li>uri:                       resource URI or connection string with all parameters in it
 * </ul>
 * <ul>
 * credential(s):
 * <li>store_key:                 (optional) a key to retrieve the credentials from {@link org.pipservices3.components.auth.ICredentialStore}
 * <li>username:                  (optional) user name
 * <li>password:                  (optional) user password
 * </ul>
 * <ul>
 * options:
 * <li>max_pool_size:             (optional) maximum connection pool size (default: 2)
 * <li>keep_alive:                (optional) enable connection keep alive (default: true)
 * <li>connect_timeout:           (optional) connection timeout in milliseconds (default: 5000)
 * <li>socket_timeout:            (optional) socket timeout in milliseconds (default: 360000)
 * <li>auto_reconnect:            (optional) enable auto reconnection (default: true)
 * <li>reconnect_interval:        (optional) reconnection interval in milliseconds (default: 1000)
 * <li>max_page_size:             (optional) maximum page size (default: 100)
 * <li>replica_set:               (optional) name of replica set
 * <li>ssl:                       (optional) enable SSL connection (default: false)
 * <li>auth_source:               (optional) authentication source
 * <li>debug:                     (optional) enable debug output (default: false).
 * </ul>
 *
 * <p>
 * ### References ###
 * <p>
 * <ul>
 * <li>*:logger:*:*:1.0           (optional) {@link org.pipservices3.components.log.ILogger} components to pass log messages
 * <li>*:discovery:*:*:1.0        (optional) {@link org.pipservices3.components.connect.IDiscovery} services
 * <li>*:credential-store:*:*:1.0 (optional) Credential stores to resolve credentials
 * </ul>
 */
public class MongoDbConnection implements IReferenceable, IConfigurable, IOpenable {

    private final ConfigParams _defaultConfig = ConfigParams.fromTuples(
            // connections.*
            // credential.*

            "options.max_pool_size", 2,
            "options.keep_alive", 1,
            "options.connect_timeout", 5000,
            "options.auto_reconnect", true,
            "options.max_page_size", 100,
            "options.debug", true
    );

    /**
     * The logger.
     */
    protected CompositeLogger _logger = new CompositeLogger();
    /**
     * The connection resolver.
     */
    protected MongoDbConnectionResolver _connectionResolver = new MongoDbConnectionResolver();
    /**
     * The configuration options.
     */
    protected ConfigParams _options = new ConfigParams();

    /**
     * The MongoDB connection object.
     */
    protected MongoClient _connection;
    /**
     * The MongoDB database name.
     */
    protected String _databaseName;
    /**
     * The MongoDb database object.
     */
    protected MongoDatabase _db;

    /**
     * Configures component by passing configuration parameters.
     *
     * @param config configuration parameters to be set.
     */
    @Override
    public void configure(ConfigParams config) {
        config = config.setDefaults(this._defaultConfig);

        this._connectionResolver.configure(config);

//        this._options = this._options.override(config.getSection("options"));
    }

    /**
     * Sets references to dependent components.
     *
     * @param references references to locate the component dependencies.
     */
    @Override
    public void setReferences(IReferences references) {
        this._logger.setReferences(references);
        this._connectionResolver.setReferences(references);
    }

    /**
     * Checks if the component is opened.
     *
     * @return true if the component has been opened and false otherwise.
     */
    @Override
    public boolean isOpen() {
        return this._connection != null;
    }

    private MongoClientOptions.Builder composeSettings() {
        var maxPoolSize = this._options.getAsIntegerWithDefault("max_pool_size", 100);
        var keepAlive = this._options.getAsIntegerWithDefault("keep_alive", 1);
        var connectTimeoutMS = this._options.getAsIntegerWithDefault("connect_timeout", _defaultConfig.getAsInteger("connect_timeout"));
        var socketTimeoutMS = this._options.getAsIntegerWithDefault("socket_timeout", _defaultConfig.getAsInteger("socket_timeout"));
        var autoReconnect = this._options.getAsBooleanWithDefault("auto_reconnect", _defaultConfig.getAsBoolean("auto_reconnect"));
        var reconnectInterval = this._options.getAsIntegerWithDefault("reconnect_interval", _defaultConfig.getAsInteger("reconnect_interval"));
        var debug = this._options.getAsBooleanWithDefault("debug", _defaultConfig.getAsBoolean("debug"));

        var ssl = this._options.getAsBoolean("ssl");
        var replicaSet = this._options.getAsNullableString("replica_set");

//            var authSource = this._options.getAsNullableString("auth_source");
//            var authUser = this._options.getAsNullableString("auth_user");
//            var authPassword = this._options.getAsNullableString("auth_password");
//            assert databaseName != null;
//            MongoCredential cred = MongoCredential.createCredential(authUser, databaseName, authPassword.toCharArray());

        Logger.getLogger("org.mongodb.driver").setLevel(!debug ? Level.OFF : Level.ALL);

        return MongoClientOptions.builder()
                .connectTimeout(connectTimeoutMS)
                .socketTimeout(socketTimeoutMS)
                .maxConnectionIdleTime(keepAlive)
                .sslEnabled(ssl)
                .requiredReplicaSetName(replicaSet)
                .maxConnectionLifeTime(maxPoolSize);
    }

    /**
     * Opens the component.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     */
    @Override
    public void open(String correlationId) throws ApplicationException {

        var uri = this._connectionResolver.resolve(correlationId);

        this._logger.debug(correlationId, "Connecting to mongodb");

        try {
            var settings = this.composeSettings();

            MongoClientURI clientUri = new MongoClientURI(uri, settings);
            String databaseName = clientUri.getDatabase();

            var connection = new MongoClient(clientUri);

            PojoCodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();
            CodecRegistry pojoCodecRegistry = CodecRegistries.fromRegistries(
                    // Custom codecs for unsupported types
                    CodecRegistries.fromCodecs(
                            new ZonedDateTimeStringCodec(),
                            new LocalDateTimeStringCodec(),
                            new LocalDateStringCodec(),
                            new DurationInt64Codec()
                    ),
                    MongoClient.getDefaultCodecRegistry(),
                    // POJO codecs to allow object serialization
                    CodecRegistries.fromProviders(pojoCodecProvider)
            );

            this._connection = connection;
            this._db = connection.getDatabase(databaseName).withCodecRegistry(pojoCodecRegistry);
            this._databaseName = this._db.getName();
        } catch (Exception ex) {
            throw new ConnectionException(
                    correlationId,
                    "CONNECT_FAILED",
                    "Connection to mongodb failed"
            ).withCause(ex);
        }
    }

    /**
     * Opens the component.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     */
    @Override
    public void close(String correlationId) {
        if (this._connection == null)
            return;

        this._connection.close();

        this._connection = null;
        this._db = null;

        this._logger.debug(correlationId, "Disconnected from mongodb database %s", this._databaseName);

        this._databaseName = null;
    }

    public MongoClient getConnection() {
        return this._connection;
    }

    public MongoDatabase getDatabase() {
        return this._db;
    }

    public String getDatabaseName() {
        return this._databaseName;
    }
}
