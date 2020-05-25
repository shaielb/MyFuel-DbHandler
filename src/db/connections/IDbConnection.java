package db.connections;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public interface IDbConnection {

	public void connect(String host, Integer port, String dbName, String userName, String password) throws SQLException;
	
	public Map<String, String> retrieveTableMetadata(String table);
	
	public ResultSet runQuery(String query) throws SQLException;
	
	public void runUpdate(String query) throws SQLException;
	
	public void close() throws SQLException;
}
