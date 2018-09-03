package org.pipservices.mongodb;

import org.pipservices.components.auth.CredentialParams;
import org.pipservices.components.auth.CredentialResolver;
import org.pipservices.commons.config.ConfigParams;
import org.pipservices.commons.config.IReconfigurable;
import org.pipservices.components.connect.ConnectionParams;
import org.pipservices.components.connect.ConnectionResolver;
import org.pipservices.commons.errors.ApplicationException;
import org.pipservices.commons.errors.ConfigException;
import org.pipservices.commons.errors.ConnectionException;
import org.pipservices.components.log.CompositeLogger;
import org.pipservices.commons.refer.IReferenceable;
import org.pipservices.commons.refer.IReferences;
import org.pipservices.commons.run.*;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.*;


public class MongoDbPersistence<T> implements IReferenceable, IReconfigurable, IOpenable, ICleanable {

	
	private ConfigParams _defaultConfig = ConfigParams.fromTuples(
            //"connection.type", "mongodb",
            //"connection.database", "test",
            //"connection.host", "localhost",
            //"connection.port", 27017,

            "options.poll_size", 4,
            "options.keep_alive", 1,
            "options.connect_timeout", 5000,
            "options.auto_reconnect", true,
            "options.max_page_size", 100,
            "options.debug", true
        );
	
	protected String _collectionName;
    protected ConnectionResolver _connectionResolver = new ConnectionResolver();
    protected CredentialResolver _credentialResolver = new CredentialResolver();
    protected ConfigParams _options = new ConfigParams();
    protected Object _lock = new Object();

    protected MongoClient _connection;
    protected MongoDatabase _database;
    protected MongoCollection<T> _collection;
    
    protected MongoClientURI clientURI;

    protected CompositeLogger _logger = new CompositeLogger();

    public MongoDbPersistence(String collectionName)
    {
        if (isNullOrWhiteSpace(collectionName))
            throw new NullPointerException(collectionName);

        _collectionName = collectionName;
    }
	
    public static boolean isNullOrWhiteSpace(String s) {
        return s == null || isWhitespace(s);
    }
    
    private static boolean isWhitespace(String s) {
        int length = s.length();
        if (length > 0) {
            for (int i = 0; i < length; i++) {
                if (!Character.isWhitespace(s.charAt(i))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
    
    public void setReferences(IReferences references)
    {
        _logger.setReferences(references);
        _connectionResolver.setReferences(references);
        _credentialResolver.setReferences(references);
    }

    public void configure(ConfigParams config)
    {
        config = config.setDefaults(_defaultConfig);

        _connectionResolver.configure(config, true);
        _credentialResolver.configure(config, true);

        _collectionName = config.getAsStringWithDefault("collection", _collectionName);

        _options = _options.override(config.getSection("options"));
    }

    
    @SuppressWarnings("unchecked")
	public void open(String correlationId, ConnectionParams connection, CredentialParams credential) throws ConnectionException, ConfigException
    {
        if (connection == null)
            throw new ConfigException(correlationId, "NO_CONNECTION", "Database connection is not set");

        String uri = connection.getUri();
        System.out.println(uri);
        String host = connection.getHost();
        int port = connection.getPort();
        String databaseName = connection.getAsNullableString("database");
        //clientURI = new MongoClientURI( "mongodb://"+ uri.substring(7));
        clientURI = new MongoClientURI("mongodb://" + host + ":" + port);
        System.out.println(clientURI.toString());
        
        if (uri != null)
        {        	      	
            //databaseName = clientURI.getURI();
        	//databaseName = clientURI.toString();
        	databaseName = clientURI.getDatabase();
            System.out.println(databaseName);
        }
        else
        {
            if (host == null)
                throw new ConfigException(correlationId, "NO_HOST", "Connection host is not set");

            if (port == 0)
                throw new ConfigException(correlationId, "NO_PORT", "Connection port is not set");

            if (databaseName == null)
                throw new ConfigException(correlationId, "NO_DATABASE", "Connection database is not set");
        }

        _logger.trace(correlationId, "Connecting to mongodb database {0}, collection {1}", databaseName, _collectionName);
        
        try
        {
            if (uri != null)
            {
            	//MongoClientURI connectionString = new MongoClientURI("mongodb://"+ uri.substring(7));
            	_connection = new MongoClient(clientURI);
            }
            _database = _connection.getDatabase(databaseName);
            _database.createCollection(_collectionName);
            _collection =  (MongoCollection<T>)_database.getCollection(_collectionName);

            _logger.debug(correlationId, "Connected to mongodb database {0}, collection {1}", databaseName, _collectionName);
        }
        catch (Exception ex)
        {
            throw new ConnectionException(correlationId, "Connection to mongodb failed", ex.toString());
        }
    }
    
    

	@Override
	public void close(String correlationId) throws ApplicationException {
		_connection.close();
	}

	@Override
	public void clear(String correlationId) throws ApplicationException {
		_collection.drop();
		
	}

	@Override
	public void open(String correlationId) throws ApplicationException {
		ConnectionParams connection = _connectionResolver.resolve(correlationId);
        CredentialParams credential = _credentialResolver.lookup(correlationId);
        open(correlationId, connection, credential);
		
	}

	@Override
	public boolean isOpened() {
		return _collection != null;
	}


}
