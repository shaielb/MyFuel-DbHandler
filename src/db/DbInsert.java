package db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import annotations.Table;
import db.connections.MySqlConnection;
import db.connections.MySqlConnection.StatementType;
import db.interfaces.IEntity;
import db.interfaces.IEntityBridge;
import db.services.Services;
import pool.ObjectPool;

class DbInsert extends DbBase {

	private ObjectPool<MySqlConnection> _connectionPool;
	
	public DbInsert(ObjectPool<MySqlConnection> connectionPool) {
		_connectionPool = connectionPool;
	}
	
	/**
	 * @param table
	 * @param entity
	 * @param entityBridge
	 * @return
	 * @throws Exception
	 */
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

	/**
	 *
	 */
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
						psToRun.setObject(index, (value instanceof IEntity) ? ((IEntity) value).getId() : value);
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
	
}
