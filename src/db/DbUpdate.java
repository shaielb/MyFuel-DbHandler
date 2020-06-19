package db;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

import annotations.Table;
import db.connections.MySqlConnection;
import db.connections.MySqlConnection.StatementType;
import db.interfaces.IEntity;
import db.interfaces.IEntityBridge;
import db.services.Services;
import pool.ObjectPool;

class DbUpdate extends DbBase {

private ObjectPool<MySqlConnection> _connectionPool;
	
	public DbUpdate(ObjectPool<MySqlConnection> connectionPool) {
		_connectionPool = connectionPool;
	}
	
	/**
	 * @param table
	 * @param entity
	 * @param entityBridge
	 * @return
	 * @throws Exception
	 */
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

	/**
	 *
	 */
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
						psToRun.setObject(index, (value instanceof IEntity) ? ((IEntity) value).getId() : value);
						i[0]++;
					}
				});
		ps.setObject(i[0] + 1, entity.getId());
		try {
			ps.executeUpdate();
		} catch (Exception e) {
			throw e;
		}
		_connectionPool.push(connection);
	}
	
	/**
	 *
	 */
	public void filteredUpdate(IEntity entity) throws Exception {
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
						psToRun.setObject(index, (value instanceof IEntity) ? ((IEntity) value).getId() : value);
						i[0]++;
					}
				});
		ps.setObject(i[0] + 1, entity.getId());
		ps.executeUpdate();
		_connectionPool.push(connection);
	}
	
}
