package db;

import java.util.List;
import java.util.Map;

import db.interfaces.IEntity;
import messages.QueryContainer;

public interface IDbComponent {
	/**
	 * @param tables
	 * @return
	 * @throws Exception
	 */
	public <TEntity extends IEntity> void cacheEntityEnums() throws Exception;
	
	/**
	 * @param tables
	 * @return
	 * @throws Exception
	 */
	public Map<String, List<IEntity>> collect(String[] tables) throws Exception;
	/**
	 * @param table
	 * @return
	 * @throws Exception
	 */
	public List<IEntity> collect(String table) throws Exception;
	/**
	 * @param queryEntitiesMap
	 * @return
	 * @throws Exception
	 */
	public List<IEntity> filter(List<QueryContainer> queryContainers) throws Exception;
	/**
	 * @param queryContainer
	 * @return
	 * @throws Exception
	 */
	public List<IEntity> filter(QueryContainer queryContainer) throws Exception;
	/**
	 * @param entity
	 * @return
	 * @throws Exception
	 */
	public List<IEntity> filter(IEntity entity) throws Exception;
	/**
	 * @param entity
	 * @return
	 * @throws Exception
	 */
	public Integer insert(IEntity entity) throws Exception;
	/**
	 * @param entity
	 * @throws Exception
	 */
	public void update(IEntity entity) throws Exception;
	/**
	 * @param entity
	 * @throws Exception
	 */
	public void remove(IEntity entity) throws Exception;
	/**
	 * @param queryEntitiesMap
	 * @return
	 * @throws Exception
	 */
	public void remove(List<QueryContainer> queryContainers) throws Exception;

	/**
	 * @param host
	 */
	public void setHost(String host);
	/**
	 * @param port
	 */
	public void setPort(Integer port);
}
