package org.pipservices.mongodb.connect;

import java.util.*;

import org.pipservices.components.auth.CredentialParams;
import org.pipservices.components.auth.CredentialResolver;
import org.pipservices.commons.config.ConfigParams;
import org.pipservices.commons.config.IConfigurable;
import org.pipservices.components.connect.ConnectionParams;
import org.pipservices.components.connect.ConnectionResolver;
import org.pipservices.commons.errors.ApplicationException;
import org.pipservices.commons.errors.ConfigException;
import org.pipservices.commons.refer.IReferenceable;
import org.pipservices.commons.refer.IReferences;

/**
 * Helper class that resolves MongoDB connection and credential parameters,
 * validates them and generates a connection URI.
 * 
 * It is able to process multiple connections to MongoDB cluster nodes.
 * 
 *  ### Configuration parameters ###
 * 
 * connection(s):
 *   discovery_key:               (optional) a key to retrieve the connection from IDiscovery
 *   host:                        host name or IP address
 *   port:                        port number (default: 27017)
 *   database:                    database name
 *   uri:                         resource URI or connection string with all parameters in it
 * credential(s):
 *   store_key:                   (optional) a key to retrieve the credentials from ICredentialStore
 *   username:                    user name
 *   password:                    user password
 * 
 * ### References ###
 * 
 * - *:discovery:*:*:1.0          (optional) IDiscovery services
 * - *:credential-store:*:*:1.0   (optional) Credential stores to resolve credentials
 */
public class MongoDbConnectionResolver implements IReferenceable, IConfigurable {

	/**
	 * The connections resolver.
	 */
	protected ConnectionResolver _connectionResolver = new ConnectionResolver();
	/**
	 * The credentials resolver.
	 */
	protected CredentialResolver _credentialResolver = new CredentialResolver();

	/**
	 * Configures component by passing configuration parameters.
	 * 
	 * @param config configuration parameters to be set.
	 */
	public void configure(ConfigParams config) {
		_connectionResolver.configure(config, false);
		_credentialResolver.configure(config, false);
	}

	/**
	 * Sets references to dependent components.
	 * 
	 * @param references references to locate the component dependencies.
	 */
	public void setReferences(IReferences references) {
		_connectionResolver.setReferences(references);
		_credentialResolver.setReferences(references);
	}

	private void validateConnection(String correlationId, ConnectionParams connection) throws ConfigException {
		String uri = connection.getUri();
		if (uri != null)
			return;

		String host = connection.getHost();
		if (host == null)
			throw new ConfigException(correlationId, "NO_HOST", "Connection host is not set");

		int port = connection.getPort();
		if (port == 0)
			throw new ConfigException(correlationId, "NO_PORT", "Connection port is not set");

		String database = connection.getAsNullableString("database");
		if (database == null)
			throw new ConfigException(correlationId, "NO_DATABASE", "Connection database is not set");
	}

	private void validateConnections(String correlationId, List<ConnectionParams> connections) throws ConfigException {
		if (connections == null || connections.size() == 0)
			throw new ConfigException(correlationId, "NO_CONNECTION", "Database connection is not set");

		for (ConnectionParams connection : connections)
			validateConnection(correlationId, connection);
	}

	private String composeUri(List<ConnectionParams> connections, CredentialParams credential) {
		// If there is a uri then return it immediately
		for (ConnectionParams connection : connections) {
			String fullUri = connection.getAsNullableString("uri");// connection.Uri;
			if (fullUri != null)
				return fullUri;
		}

		// Define hosts
		String hosts = "";
		for (ConnectionParams connection : connections) {
			String host = connection.getHost();
			int port = connection.getPort();

			if (hosts.length() > 0)
				hosts += ",";
			hosts += host + (port == 0 ? "" : ":" + port);
		}

		// Define database
		String database = "";
		for (ConnectionParams connection : connections) {
			database = connection.getAsNullableString("database") != null ? connection.getAsNullableString("database")
					: database;
		}

		if (database.length() > 0)
			database = "/" + database;

		// Define authentication part
		String auth = "";
		if (credential != null) {
			String username = credential.getUsername();
			if (username != null) {
				String password = credential.getPassword();
				if (password != null)
					auth = username + ":" + password + "@";
				else
					auth = username + "@";
			}
		}

		// Define additional parameters parameters
		ConfigParams options = new ConfigParams();
		for (ConnectionParams connection : connections)
			options = options.override(connection);
		if (credential != null)
			options = options.override(credential);

		options.remove("uri");
		options.remove("host");
		options.remove("port");
		options.remove("database");
		options.remove("username");
		options.remove("password");

		String parameters = "";
		Set<String> keys = options.keySet();
		for (String key : keys) {
			if (parameters.length() > 0)
				parameters += "&";

			parameters += key;

			String value = options.getAsString(key);
			if (value != null)
				parameters += "=" + value;
		}

		if (parameters.length() > 0)
			parameters = "?" + parameters;

		// Compose uri
		String uri = "mongodb://" + auth + hosts + database + parameters;

		return uri;
	}

	/**
	 * Resolves MongoDB connection URI from connection and credential parameters.
	 * 
	 * @param correlationId (optional) transaction id to trace execution through
	 *                      call chain.
	 * @return resolved URI.
	 * @throws ApplicationException when error occured.
	 */
	public String resolve(String correlationId) throws ApplicationException {
		List<ConnectionParams> connections = _connectionResolver.resolveAll(correlationId);
		CredentialParams credential = _credentialResolver.lookup(correlationId);

		validateConnections(correlationId, connections);

		return composeUri(connections, credential);
	}

}
