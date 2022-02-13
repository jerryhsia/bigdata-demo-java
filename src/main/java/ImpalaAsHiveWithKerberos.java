import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ImpalaAsHiveWithKerberos {
    public static void main(String[] args) throws Exception {
        // KrbUtil.initKrbHiveAuth1();
        KrbUtil.initKrbHiveAuth2();

        // 这个地址填写自己的impala server地址,默认端口为21050，请按需修改
        String connectionUrl = "jdbc:hive2://10.36.248.27:8703/default;AuthMech=1;principal=impala/quickstart.cloudera@HADOOP.COM";
        String jdbcDriverName = "org.apache.hive.jdbc.HiveDriver";
        String sqlStatement = "show tables";

        Class.forName(jdbcDriverName);
        Connection con = DriverManager.getConnection(connectionUrl);
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(sqlStatement);

        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }
}