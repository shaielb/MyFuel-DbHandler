package db;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import annotations.Table;
import db.connections.MySqlConnection;
import db.interfaces.IEntity;
import db.interfaces.IEntityBridge;
import db.interfaces.IEnum;
import db.services.Services;
import globals.Globals;
import pool.ObjectPool;
import utilities.Cache;

@SuppressWarnings("unchecked")
class DbCollect extends DbBase {

	private ObjectPool<MySqlConnection> _connectionPool;

	public DbCollect(ObjectPool<MySqlConnection> connectionPool) {
		_connectionPool = connectionPool;
	}

	/**
	 * @param connection
	 * @param table
	 * @param where
	 * @return
	 * @throws Exception
	 */
	public List<IEntity> collect(MySqlConnection connection, String table, String where) throws Exception {
		List<IEntity> list = new ArrayList<IEntity>();
		IEntityBridge entityBridge = Services.getBridge(table);
		ResultSet rs = connection.runQuery(String.format("select * from %s %s", table, where));

		Map<String, String>[] fkMap = new HashMap[1];

		while (rs.next()) {
			IEntity newEntity = entityBridge.createEntity();
			entityBridge.populateEntity(newEntity, 
					(index, name, value) -> {
						Object res = rs.getObject(index + 1);
						if (name.endsWith("_fk")) {
							if (fkMap[0] == null) {
								fkMap[0] = connection.retrieveTableFkMap(table);
							}
							if (name.endsWith("_enum_fk")) {
								return getEnum(name.substring(0, name.lastIndexOf("_")), (Integer) res);
							}
							else {
								try {
									IEntity entity = Services.getBridge(fkMap[0].get(name)).createEntity();
									entity.setId((Integer) res);
									return entity;
								} catch (Exception e) {
									e.printStackTrace();
								}
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

	/**
	 *
	 */
	public List<IEntity> collect(String table) throws Exception {
		return collect(new String[] { table }).get(table);
	}

	/**
	 *
	 */
	public Map<String, List<IEntity>> collect(String[] tables) throws Exception {
		MySqlConnection connection = _connectionPool.pop();
		Map<String, List<IEntity>> tablesMap = new HashMap<String, List<IEntity>>();
		for (int i = 0; i < tables.length; ++i) {
			String table = tables[i];
			tablesMap.put(table, collect(connection, table, ""));
		}
		_connectionPool.push(connection);
		return tablesMap;
	}

	/**
	 *
	 */
	public IEntity getEnum(String enumTable, Integer id) {
		Map<String, List<IEntity>> enumsTables = (Map<String, List<IEntity>>) Cache.get(Globals.EnumTables);
		List<IEntity> entities = enumsTables.get(enumTable);
		if (entities != null) {
			for (IEntity entity : entities) {
				if (entity instanceof IEnum) {
					if (((IEntity)entity).getId().equals(id)) {
						return entity;
					}
				}
			}
		}
		return  null;
	}

	/**
	 *
	 */
	public IEntity getEnum(String enumTable, String keyValue) {
		Map<String, List<IEntity>> enumsTables = (Map<String, List<IEntity>>) Cache.get(Globals.EnumTables);
		List<IEntity> entities = enumsTables.get(enumTable);
		if (entities != null) {
			for (IEntity entity : entities) {
				if (entity instanceof IEnum) {
					if (((IEnum)entity).getKey().toLowerCase().equals(keyValue.toLowerCase())) {
						return entity;
					}
				}
			}
		}
		return  null;
	}

	/**
	 * @param enumTable
	 * @param keyValue
	 * @return
	 */
	public <TEntity extends IEntity> TEntity getEnum(Class<TEntity> enumClass, String keyValue) {
		String enumTable = enumClass.getAnnotation(Table.class).Name();
		Map<String, List<IEntity>> enumsTables = (Map<String, List<IEntity>>) Cache.get(Globals.EnumTables);
		List<IEntity> entities = enumsTables.get(enumTable);
		if (entities != null) {
			for (IEntity entity : entities) {
				if (entity instanceof IEnum) {
					if (((IEnum)entity).getKey().toLowerCase().equals(keyValue.toLowerCase())) {
						return (TEntity) entity;
					}
				}
			}
		}
		return  null;
	}
}
