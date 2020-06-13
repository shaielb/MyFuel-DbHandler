package db.connections;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public interface IDbConnection {

	/**
	 * @param host
	 * @param port
	 * @param dbName
	 * @param userName
	 * @param password
	 * @throws SQLException
	 */
	public void connect(String host, Integer port, String dbName, String userName, String password) throws SQLException;
	
	/**
	 * @param table
	 * @return
	 */
	public Map<String, String> retrieveTableMetadata(String table);
	
	/**
	 * @param query
	 * @return
	 * @throws SQLException
	 */
	public ResultSet runQuery(String query) throws SQLException;
	
	/**
	 * @param query
	 * @throws SQLException
	 */
	public void runUpdate(String query) throws SQLException;
	
	/**
	 * @throws SQLException
	 */
	public void close() throws SQLException;
}
