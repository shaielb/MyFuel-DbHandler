package db;

import java.util.ArrayList;
import java.util.List;

import db.DbComponent.DbAction;
import db.connections.MySqlConnection;
import db.interfaces.IEntity;
import messages.QueryContainer;
import pool.ObjectPool;

@SuppressWarnings("serial")
class DbFilter extends DbBase {

	private ObjectPool<MySqlConnection> _connectionPool;
	
	private DbCollect _dbCollect;
	
	public DbFilter(ObjectPool<MySqlConnection> connectionPool, DbCollect dbCollect) {
		_connectionPool = connectionPool;
		_dbCollect = dbCollect;
	}
	
	/**
	 *
	 */
	public List<IEntity> filter(IEntity entity) throws Exception {
		QueryContainer container = new QueryContainer();
		container.setQueryEntity(entity);
		return filter(container);
	}

	/**
	 *
	 */
	public List<IEntity> filter(QueryContainer queryContainer) throws Exception {
		return filter(new ArrayList<QueryContainer>() {{
			add(queryContainer);
		}});
	}

	/**
	 *
	 */
	public List<IEntity> filter(List<QueryContainer> queryContainers) throws Exception {
		MySqlConnection connection = _connectionPool.pop();
		DbAction dbAction = (conn, table, where) -> { return _dbCollect.collect(conn, table, where); };
		List<IEntity> resultsList = filter(queryContainers, connection, dbAction);
		_connectionPool.push(connection);
		return resultsList;
	}
	
}
