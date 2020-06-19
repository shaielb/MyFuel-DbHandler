package db;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import configuration.Configuration;
import db.connections.MySqlConnection;
import db.interfaces.IEntity;
import globals.Globals;
import messages.QueryContainer;
import pool.ObjectPool;
import utilities.Cache;
import utilities.EnvUtil;

@SuppressWarnings({ "unchecked" })
public class DbComponent implements IDbComponent {
	public interface DbAction {
		public List<IEntity> execute(MySqlConnection connection, String table, String where) throws Exception;
	}

	/**
	 * 
	 */
	public static final String DefaultHost = "localhost";
	/**
	 * 
	 */
	public static final Integer DefaultPort = 3306;

	/**
	 * 
	 */
	public static final String DefaultDbName = "myfuel";
	/**
	 * 
	 */
	public static final String DefaultUserName = "myfuel";
	/**
	 * 
	 */
	public static final String DefaultPassword = "1234";

	/**
	 * 
	 */
	private String _host;
	/**
	 * 
	 */
	private int _port;

	/**
	 * 
	 */
	private String _dbName;
	/**
	 * 
	 */
	private String _userName;
	/**
	 * 
	 */
	private String _password;

	/**
	 * 
	 */
	private ObjectPool<MySqlConnection> _connectionPool;

	private DbCollect _dbCollect;
	private DbFilter _dbFilter;
	private DbInsert _dbInsert;
	private DbUpdate _dbUpdate;
	private DbRemove _dbRemove;

	/**
	 * 
	 */
	public DbComponent() {
		_host = DefaultHost;
		_port = DefaultPort;
		_dbName = DefaultDbName;
		_userName = DefaultUserName;
		_password = DefaultPassword;

		checkArgsFromConfigurationFile();

		_connectionPool = 
				new ObjectPool<MySqlConnection>(EnvUtil.getCoresNumber(), () ->  {
					MySqlConnection connection = new MySqlConnection();
					try {
						connection.connect(_host, _port, _dbName, _userName, _password);
					} catch (SQLException e) {
						e.printStackTrace();
						return null;
					}
					return connection;
				});

		_dbCollect = new DbCollect(_connectionPool);
		_dbFilter = new DbFilter(_connectionPool, _dbCollect);
		_dbInsert = new DbInsert(_connectionPool);
		_dbUpdate = new DbUpdate(_connectionPool);
		_dbRemove = new DbRemove(_connectionPool);
	}

	/**
	 * 
	 */
	private void checkArgsFromConfigurationFile() {
		String localPath = null;
		try {
			localPath = Configuration.class.getProtectionDomain().getCodeSource().getLocation()
					.toURI().toString();
			localPath = localPath.substring(localPath.indexOf("/") + 1);
			localPath = localPath.substring(0, localPath.lastIndexOf("/") + 1);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		Map<String, Object> configuration = Configuration.configuration(localPath + "Configurations/configuration.xml");
		if (configuration != null) {
			Object dbParameters = configuration.get("dbParameters");
			if (dbParameters != null) {
				Map<String, Object> dbParams = (Map<String, Object>) dbParameters;
				Object host = dbParams.get("host");
				if (host != null) {
					_host = (String) host;
				}
				Object port = dbParams.get("port");
				if (port != null) {
					_port = Integer.parseInt((String) port);
				}
				Object dbName = dbParams.get("dbName");
				if (dbName != null) {
					_dbName = (String) dbName;
				}
				Object userName = dbParams.get("userName");
				if (userName != null) {
					_userName = (String) userName;
				}
				Object password = dbParams.get("password");
				if (password != null) {
					_password = (String) password;
				}
			}
		}
	}

	/**
	 *
	 */
	@Override
	public <TEntity extends IEntity> void cacheEntityEnums() throws Exception {
		List<String> enumTables = new ArrayList<String>();
		MySqlConnection connection = _connectionPool.pop();
		ResultSet rs = connection.runQuery("show tables");
		_connectionPool.push(connection);
		while (rs.next()) {
			String res = (String) rs.getObject(1);
			if (res.endsWith("_enum")) {
				enumTables.add(res);
			}
		}
		rs.close();
		cacheTables(enumTables);
	}

	/**
	 * @throws Exception 
	 *
	 */
	public void cacheTables(Collection<String> tables) throws Exception {
		if (tables.size() > 0) {
			Map<String, List<IEntity>> enumsTables = (Map<String, List<IEntity>>) Cache.get(Globals.EnumTables);
			if (enumsTables == null) {
				Cache.put(Globals.EnumTables, enumsTables = new HashMap<String, List<IEntity>>());
			}
			enumsTables.putAll(_dbCollect.collect(tables.toArray(new String[tables.size()])));
		}
	}

	@Override
	public Map<String, List<IEntity>> collect(String[] tables) throws Exception {
		return _dbCollect.collect(tables);
	}

	@Override
	public List<IEntity> collect(String table) throws Exception {
		return _dbCollect.collect(table);
	}

	@Override
	public List<IEntity> filter(List<QueryContainer> queryContainers) throws Exception {
		return _dbFilter.filter(queryContainers);
	}

	@Override
	public List<IEntity> filter(QueryContainer queryContainer) throws Exception {
		return _dbFilter.filter(queryContainer);
	}

	@Override
	public List<IEntity> filter(IEntity entity) throws Exception {
		return _dbFilter.filter(entity);
	}

	@Override
	public Integer insert(IEntity entity) throws Exception {
		return _dbInsert.insert(entity);
	}

	@Override
	public void update(IEntity entity) throws Exception {
		_dbUpdate.update(entity);
	}

	@Override
	public void remove(IEntity entity) throws Exception {
		_dbRemove.remove(entity);
	}

	@Override
	public void remove(List<QueryContainer> queryContainers) throws Exception {
		_dbRemove.remove(queryContainers);
	}

	/**
	 *
	 */
	@Override
	public void setHost(String host) {
		_host = host;
	}

	/**
	 *
	 */
	@Override
	public void setPort(Integer port) {
		_port = port;
	}
}
