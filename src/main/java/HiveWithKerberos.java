import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
public class HiveWithKerberos {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws Exception {
        KrbUtil.initKrbHiveAuth1();
        // KrbUtil.initKrbHiveAuth2();

        String JDBC_DB_URL = "jdbc:hive2://10.36.248.27:8719/default;principal=hive/quickstart.cloudera@HADOOP.COM";

        Class.forName(driverName);
        Connection con = DriverManager.getConnection(JDBC_DB_URL);
        Statement stmt = con.createStatement();
        String sql = "show tables";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }

        res.close();
        stmt.close();
        con.close();
    }
}
