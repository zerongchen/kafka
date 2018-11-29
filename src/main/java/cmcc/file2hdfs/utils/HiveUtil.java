package cmcc.file2hdfs.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

public class HiveUtil {
	
	private static Logger logger = Logger.getLogger(HiveUtil.class);

	public static Connection getHiveConnection() {
		CmccConfig config = CmccConfig.getInstance();
		
		Connection connection = null;
			
		try {
			Class.forName(config.getHiveJdbcDriver());
			connection = DriverManager.getConnection(
					config.getHiveJdbcUrl(),
					config.getHiveJdbcUser(),
					config.getHiveJdbcPasswd());
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		return connection;
	}
	
	/**
	 * 关闭一个数据库连接
	 * @param conn
	 */
	public static void closeConnection(Connection conn){
		if(conn != null){
			try {
				conn.close();
			} catch (SQLException e) {
				logger.error("Close Connect fail");
			}
			conn = null;
		}
	}
	
	// 关闭数据库连接，释放回数据库连接池
	public static void closeConnection(Connection conn, java.sql.Statement st,
			ResultSet rs) {
		try {
			if (conn != null) {
				conn.close();
			}
			conn = null;

			if (st != null) {
				st.close();
			}
			st = null;
			if (rs != null) {
				rs.close();
			}
			rs = null;
		} catch (Exception ex) {
		}
	}
	
	// 关闭数据库连接，释放回数据库连接池
	public static void closeConnection(Connection conn, java.sql.Statement st) {
		try {
			if (conn != null) {
				conn.close();
			}
			conn = null;

			if (st != null) {
				st.close();
			}
			st = null;
		} catch (Exception ex) {
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		Connection connection = HiveUtil.getHiveConnection();
		String sql = "alter table original_bill add if not exists partition (BillType = 2,partdate=20161027) location 'billtype=2/partdate=20161027'";
		Statement stmt = null;
		try {
			stmt = connection.createStatement();
			stmt.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			if(stmt != null){
				try {
					stmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				stmt = null;
			}
		}
        
	}

}
