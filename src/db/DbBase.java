package db;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import annotations.Table;
import comperators.Comperators;
import db.DbComponent.DbAction;
import db.connections.MySqlConnection;
import db.interfaces.IEntity;
import db.interfaces.IEntityBridge;
import db.services.Services;
import messages.QueryContainer;

@SuppressWarnings("serial")
class DbBase {

	/**
	 * 
	 */
	public static final String StringValueSign = "<Value>";
	
	/**
	 * 
	 */
	private Map<String, String> _comperatorsMap = new HashMap<String, String>() {{
		put(Comperators.StartsWith, "'" + StringValueSign  + "%'");
		put(Comperators.EndsWith, "'%" + StringValueSign  + "'");
		put(Comperators.Containes, "'%" + StringValueSign  + "%'");
	}};
	
	/**
	 * 
	 * @param queryContainers
	 * @param connection
	 * @return
	 * @throws Exception
	 */
	public List<IEntity> filter(List<QueryContainer> queryContainers, MySqlConnection connection, DbAction dbAction) throws Exception {
		Map<String, List<QueryContainer>> map = new HashMap<String, List<QueryContainer>>();
		for (QueryContainer container : queryContainers) {
			IEntity entity = container.getQueryEntity();
			String table = entity.getClass().getAnnotation(Table.class).Name();
			List<QueryContainer> list = map.get(table);
			if (list == null) {
				map.put(table, list = new ArrayList<QueryContainer>());
			}
			list.add(container);
		}

		List<IEntity> resultsList = new ArrayList<IEntity>();
		for (Entry<String, List<QueryContainer>> entry : map.entrySet()) {
			List<IEntity> list = filterByEntityType(entry.getValue(), connection, dbAction);
			resultsList.addAll(list);
		}
		return resultsList;
	}
	
	/**
	 * 
	 * @param queryContainers
	 * @param connection
	 * @return
	 * @throws Exception
	 */
	protected List<IEntity> filterByEntityType(List<QueryContainer> queryContainers, MySqlConnection connection, 
			DbAction dbAction) throws Exception {
		Map<String, List<String>> whereMap = new HashMap<String, List<String>>();

		for (QueryContainer container : queryContainers) {
			IEntity entity = container.getQueryEntity();
			Map<String, String> querySigns = container.getQueryMap();
			String table = entity.getClass().getAnnotation(Table.class).Name();
			List<String> whereList = whereMap.get(table);
			if (whereList == null) {
				whereMap.put(table, whereList = new ArrayList<String>());
			}
			IEntityBridge entityBridge = Services.getBridge(table);
			List<String> whereListF = whereList;
			entityBridge.collectFromEntity(entity, 
					(index, name, value) -> {
						String querySign = querySigns.get(name);
						if (querySign != null && value != null) {
							String valueStr = "";
							if (_comperatorsMap.keySet().contains(querySign)) {
								valueStr = _comperatorsMap.get(querySign).replace(StringValueSign, (String) value);
								querySign = "like";
							}
							else if (value instanceof String || value instanceof Timestamp) {
								valueStr = "'" + value + "'";
							}
							else if (value instanceof IEntity) {
								valueStr = ((IEntity) value).getId().toString();
							}
							else {
								valueStr = value.toString();
							}
							whereListF.add(String.format("%s %s %s", name, querySign, valueStr));
						}
					});
		}
		List<IEntity> resultsList = new ArrayList<IEntity>();
		for (Entry<String, List<String>> entry : whereMap.entrySet()) {
			String table = entry.getKey();
			List<String> whereList = entry.getValue();
			String where = whereList.size() > 0 ? ("where " + String.join(" and ", whereList)) : "";
			List<IEntity> list = dbAction.execute(connection, table, where);
			if (list.size() == 1) {
				// going over dependent queries and executing them
				IEntity entity = list.get(0);
				for (QueryContainer container : queryContainers) {
					if (container.getNext() != null && container.getNext().size() > 0) {
						for (QueryContainer qc : container.getNext()) {
							IEntity nextEntity = qc.getQueryEntity();
							String nextTable = nextEntity.getClass().getAnnotation(Table.class).Name();
							IEntityBridge nextEntityBridge = Services.getBridge(nextTable);
							boolean[] found = new boolean[] { false };
							// checking to see if next entity point to the previous entity
							nextEntityBridge.populateEntity(nextEntity, 
									(index, name, value) -> {
										if (name.equals(table + "_fk")) {
											found[0] = true;
											return entity;
										}
										return value;
									});
							if (!found[0]) {
								// checking to see if previous entity point to the next entity
								IEntityBridge entityBridge = Services.getBridge(table);
								entityBridge.populateEntity(entity, 
										(index, name, value) -> {
											if (name.equals(nextTable + "_fk")) {
												found[0] = true;
												nextEntity.setId(((IEntity) value).getId());
												return nextEntity;
											}
											return value;
										});
							}
						}
						list.addAll(filter(container.getNext(), connection, dbAction));
					}
				}
			}

			resultsList.addAll(list);
		}
		return resultsList;
	}
}
