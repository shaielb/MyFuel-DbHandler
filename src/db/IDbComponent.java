package db;

import java.util.List;
import java.util.Map;

import db.interfaces.IEntity;

public interface IDbComponent {
	public Map<String, List<IEntity>> collect(String[] tables) throws Exception;
	public List<IEntity> filter(IEntity entity, Map<String, String> querySigns) throws Exception;
	public Integer insert(IEntity entity) throws Exception;
	public void update(IEntity entity) throws Exception;
	public void remove(IEntity entity) throws Exception;
	
	public void setHost(String host);
	public void setPort(Integer port);
}
