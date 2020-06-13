package db.connections;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import utilities.StringUtil;

public class MySqlConnection implements IDbConnection {

	public static enum StatementType {
		Insert, Update, Remove
	};

	/**
	 * 
	 */
	private Connection _connation;

	/**
	 * 
	 */
	private Map<StatementType, Map<String, PreparedStatement>> _preparedStatementMap = new HashMap<StatementType, Map<String, PreparedStatement>>();

	/**
	 * 
	 */
	public MySqlConnection() {
		_preparedStatementMap.put(StatementType.Insert, new HashMap<String, PreparedStatement>());
		_preparedStatementMap.put(StatementType.Update, new HashMap<String, PreparedStatement>());
		_preparedStatementMap.put(StatementType.Remove, new HashMap<String, PreparedStatement>());
	}

	/**
	 *
	 */
	@Override
	public void connect(String host, Integer port, String dbName, String userName, String password)
			throws SQLException {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			_connation = DriverManager.getConnection(
					String.format("jdbc:mysql://%s:%d/%s?serverTimezone=IST", host, port, dbName), userName, password);
			// _connation = DriverManager.getConnection("jdbc:mysql://" + host +
			// ":3306/myfuel?serverTimezone=IST", userName, password);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @param table
	 * @return
	 */
	public Map<String, String> retrieveTableFkMap(String table) {
		Map<String, String> fkMap = null;
		try {
			DatabaseMetaData meta = _connation.getMetaData();
			ResultSet rsMeta = meta.getImportedKeys("", "", table);
			fkMap = new HashMap<String, String>();
			while (rsMeta.next()) {
				String fkTableName = rsMeta.getString("PKTABLE_NAME");
				String fkColumnName = rsMeta.getString("FKCOLUMN_NAME");
				fkMap.put(fkColumnName, fkTableName);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return fkMap;
	}

	/**
	 *
	 */
	public Map<String, String> retrieveTableMetadata(String table) {
		Map<String, String> map = null;
		try {
			Statement stmt = _connation.createStatement();
			Map<String, String> fkMap = retrieveTableFkMap(table);

			ResultSet rs = stmt.executeQuery(String.format("select `*` from `%s` limit 0", table));
			ResultSetMetaData rsmt = rs.getMetaData();
			map = new LinkedHashMap<String, String>();

			int count = rsmt.getColumnCount() + 1;
			for (int i = 1; i < count; ++i) {
				String colName = rsmt.getColumnLabel(i);
				String type = rsmt.getColumnClassName(i);
				String foreignTable = fkMap.get(colName);
				if (foreignTable != null) {
					String className = StringUtil.swithToUpperCase(foreignTable, "_");
					map.put(colName, String.format("db.entity.%s", className));
				} else {
					map.put(colName, type);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return map;
	}

	/**
	 *
	 */
	public ResultSet runQuery(String query) throws SQLException {
		Statement stmt = _connation.createStatement();
		return stmt.executeQuery(query);
	}

	/**
	 *
	 */
	public void runUpdate(String query) throws SQLException {
		Statement stmt = _connation.createStatement();
		stmt.executeUpdate(query);
	}

	/**
	 * @param table
	 * @param where
	 * @return
	 * @throws SQLException
	 */
	public ResultSet collect(String table, String where) throws SQLException {
		return runQuery(String.format("select * from %s where %s;", table, where));
	}

	/**
	 * @param type
	 * @param table
	 * @return
	 * @throws SQLException
	 */
	public PreparedStatement getPreparedStatement(StatementType type, String table) throws SQLException {
		Map<String, PreparedStatement> tableStatementMap = _preparedStatementMap.get(type);
		return tableStatementMap.get(table);
	}

	/**
	 * @param type
	 * @param table
	 * @param qery
	 * @return
	 * @throws SQLException
	 */
	public PreparedStatement createPreparedStatement(StatementType type, String table, String qery)
			throws SQLException {
		Map<String, PreparedStatement> tableStatementMap = _preparedStatementMap.get(type);
		if (tableStatementMap == null) {
			_preparedStatementMap.put(type, tableStatementMap = new HashMap<String, PreparedStatement>());
		}
		PreparedStatement ps = _connation.prepareStatement(qery, PreparedStatement.RETURN_GENERATED_KEYS);
		tableStatementMap.put(table, ps);
		return ps;
	}

	/**
	 *
	 */
	public void close() throws SQLException {
		_connation.close();
	}
}
