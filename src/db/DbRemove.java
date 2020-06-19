package db;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import annotations.Table;
import db.DbComponent.DbAction;
import db.connections.MySqlConnection;
import db.connections.MySqlConnection.StatementType;
import db.interfaces.IEntity;
import messages.QueryContainer;
import pool.ObjectPool;

@SuppressWarnings("unchecked")
class DbRemove extends DbBase {

	private ObjectPool<MySqlConnection> _connectionPool;

	public DbRemove(ObjectPool<MySqlConnection> connectionPool) {
		_connectionPool = connectionPool;
	}

	/**
	 *
	 */
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

	private List<IEntity> remove(MySqlConnection connection, String table, String where) throws Exception {
		List<IEntity> list = new ArrayList<IEntity>();
		connection.runUpdate(String.format("delete from %s %s", table, where));
		return list;
	}

	public void remove(List<QueryContainer> queryContainers) throws Exception {
		MySqlConnection connection = _connectionPool.pop();
		remove(queryContainers, connection);
		_connectionPool.push(connection);
	}

	/**
	 * 
	 * @param queryContainers
	 * @param connection
	 * @return
	 * @throws Exception
	 */
	public void remove(List<QueryContainer> queryContainers, MySqlConnection connection) throws Exception {
		Map<String, List<QueryContainer>> map = new HashMap<String, List<QueryContainer>>();
		Set<String> tables = new HashSet<String>();
		for (QueryContainer container : queryContainers) {
			IEntity entity = container.getQueryEntity();
			String table = entity.getClass().getAnnotation(Table.class).Name();
			List<QueryContainer> list = map.get(table);
			if (list == null) {
				map.put(table, list = new ArrayList<QueryContainer>());
			}
			list.add(container);
			tables.add(table);
		}

		for (Entry<String, List<QueryContainer>> entry : map.entrySet()) {
			DbAction dbAction = (conn, table, where) -> { return remove(conn, table, where); };
			filterByEntityType(entry.getValue(), connection, dbAction);
		}
	}
}
