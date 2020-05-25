package db;

import java.net.URISyntaxException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import annotations.Table;
import comperators.Comperators;
import configuration.Configuration;
import db.connections.MySqlConnection;
import db.connections.MySqlConnection.StatementType;
import db.interfaces.IEntity;
import db.interfaces.IEntityBridge;
import db.services.Services;
import pool.ObjectPool;
import utilities.EnvUtil;

@SuppressWarnings({ "unchecked", "serial" })
public class DbComponent implements IDbComponent {
	public static final String DefaultHost = "localhost";
	public static final Integer DefaultPort = 3306;

	public static final String DefaultDbName = "myfuel";
	public static final String DefaultUserName = "myfuel";
	public static final String DefaultPassword = "1234";
	
	public static final String StringValueSign = "<Value>";

	private String _host;
	private int _port;

	private String _dbName;
	private String _userName;
	private String _password;

	private ObjectPool<MySqlConnection> _connectionPool;
	
	private Map<String, String> _comperatorsMap = new HashMap<String, String>() {{
		put(Comperators.StartsWith, "'" + StringValueSign  + "%'");
		put(Comperators.EndsWith, "'%" + StringValueSign  + "'");
		put(Comperators.Containes, "'%" + StringValueSign  + "%'");
	}};

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
	}

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
		Map<String, Object> configuration = Configuration.configuration(localPath);
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

	private List<IEntity> collect(MySqlConnection connection, String table, String where) throws Exception {
		List<IEntity> list = new ArrayList<IEntity>();
		IEntityBridge entityBridge = Services.getBridge(table);
		ResultSet rs = connection.runQuery(String.format("select * from %s %s", table, where));

		Map<String, String>[] fkMap = new HashMap[1];

		while (rs.next()) {
			IEntity newEntity = Services.createEntity(table);
			entityBridge.populateEntity(newEntity, 
					(index, name) -> {
						Object res = rs.getObject(index + 1);
						if (name.endsWith("_fk")) {
							if (fkMap[0] == null) {
								fkMap[0] = connection.retrieveTableFkMap(table);
							}
							try {
								IEntity entity = Services.createEntity(fkMap[0].get(name));
								entity.setId((Integer) res);
								return entity;
							} catch (Exception e) {
								e.printStackTrace();
							}
							return null;
						}
						return res;
					});
			list.add(newEntity);
		}
		rs.close();
		return list;
	}
	
	@Override
	public Map<String, List<IEntity>> collect(String[] tables) throws Exception {
		MySqlConnection connection = _connectionPool.pop();
		Map<String, List<IEntity>> tablesMap = new HashMap<String, List<IEntity>>();
		for (String table : tables) {
			tablesMap.put(table, collect(connection, table, ""));
		}
		_connectionPool.push(connection);
		return tablesMap;
	}

	@Override
	public List<IEntity> filter(IEntity entity, Map<String, String> querySigns) throws Exception {
		String table = entity.getClass().getAnnotation(Table.class).Name();
		IEntityBridge entityBridge = Services.getBridge(table);
		MySqlConnection connection = _connectionPool.pop();
		List<String> whereList = new ArrayList<String>();
		entityBridge.collectFromEntity(entity, 
				(index, name, value) -> {
					String querySign = querySigns.get(name);
					if (querySign != null && value != null) {
						String valueStr = "";
						if (_comperatorsMap.keySet().contains(querySign)) {
							valueStr = _comperatorsMap.get(querySign).replace(StringValueSign, (String) value);
							querySign = "like";
						}
						else if (value instanceof String) {
							valueStr = "'" + value + "'";
						}
						else if (value instanceof IEntity) {
							valueStr = ((IEntity) value).getId().toString();
						}
						else {
							valueStr = value.toString();
						}
						whereList.add(String.format("%s %s %s", name, querySign, valueStr));
					}
				});
		String where = whereList.size() > 0 ? ("where " + String.join(" and ", whereList)) : "";
		List<IEntity> list = collect(connection, table, where);

		_connectionPool.push(connection);
		return list;
	}

	private String createInsertStr(String table, IEntity entity, IEntityBridge entityBridge) throws Exception { 
		List<String> values = new ArrayList<String>();
		List<String> names = new ArrayList<String>();
		entityBridge.collectFromEntity(entity, 
				(index, name, value) -> {
					if (!"id".equals(name)) {
						names.add(name);
						values.add("?");
					}
				});
		return String.format("insert into %s (%s) values (%s)", 
				table, String.join(",", names), String.join(",", values));
	}

	@Override
	public Integer insert(IEntity entity) throws Exception {
		String table = entity.getClass().getAnnotation(Table.class).Name();
		IEntityBridge entityBridge = Services.getBridge(table);
		MySqlConnection connection = _connectionPool.pop();
		PreparedStatement ps = connection.getPreparedStatement(StatementType.Insert, table);
		if (ps == null) {
			String insertStr = createInsertStr(table, entity, entityBridge);
			ps = connection.createPreparedStatement(StatementType.Insert, table, insertStr);
		}
		PreparedStatement psToRun = ps;
		entityBridge.collectFromEntity(entity, 
				(index, name, value) -> {
					if (index > 0) {
						psToRun.setObject(index, value);
					}
				});
		ps.executeUpdate();
		try (ResultSet generatedKeys = ps.getGeneratedKeys()) {
			if (generatedKeys.next()) {
				Integer generatedKey = generatedKeys.getInt(1);
				entity.setId(generatedKey);
				_connectionPool.push(connection);
				return generatedKey;
			}
		}
		_connectionPool.push(connection);
		return -1;
	}

	private String createUpdateStr(String table, IEntity entity, IEntityBridge entityBridge) throws Exception {
		List<String> values = new ArrayList<String>();
		entityBridge.collectFromEntity(entity, 
				(index, name, value) -> {
					if (!"id".equals(name)) {
						values.add(String.format("%s = ?", name));
					}
				});
		return String.format("update %s set %s where id = ?;", 
				table, String.join(",", values));
	}

	@Override
	public void update(IEntity entity) throws Exception {
		String table = entity.getClass().getAnnotation(Table.class).Name();
		IEntityBridge entityBridge = Services.getBridge(table);
		MySqlConnection connection = _connectionPool.pop();
		PreparedStatement ps = connection.getPreparedStatement(StatementType.Update, table);
		if (ps == null) {
			String updateStr = createUpdateStr(table, entity, entityBridge);
			ps = connection.createPreparedStatement(StatementType.Update, table, updateStr);
		}
		PreparedStatement psToRun = ps;
		int[] i = new int[] {0};
		entityBridge.collectFromEntity(entity, 
				(index, name, value) -> {
					if (!"id".equals(name)) {
						psToRun.setObject(index, value);
						i[0]++;
					}
				});
		ps.setObject(i[0] + 1, entity.getId());
		ps.executeUpdate();
		_connectionPool.push(connection);
	}

	@Override
	public void remove(IEntity entity) throws Exception {
		String table = entity.getClass().getAnnotation(Table.class).Name();
		MySqlConnection connection = _connectionPool.pop();
		PreparedStatement ps = connection.getPreparedStatement(StatementType.Remove, table);
		if (ps == null) {
			String removeStr = String.format("delete from %s where id = ?;", table);
			ps = connection.createPreparedStatement(StatementType.Remove, table, removeStr);
		}
		ps.setObject(1, entity.getId());
		ps.executeUpdate();
		_connectionPool.push(connection);
	}

	@Override
	public void setHost(String host) {
		_host = host;
	}

	@Override
	public void setPort(Integer port) {
		_port = port;
	}
}
