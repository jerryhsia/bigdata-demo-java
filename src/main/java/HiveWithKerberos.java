import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.sql.*;

public class HiveWithKerberos {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Configuration conf = new Configuration();
        System.setProperty("hive_keytab", "/Users/xiajie01/Develop/acg/bigdata-demo-java/hive_user2.keytab");
        System.setProperty("java.security.krb5.conf", "/Users/xiajie01/Develop/acg/bigdata-demo-java/krb5.conf");
        conf.set("hadoop.security.authentication", "Kerberos");
        //conf.set("hadoop.security.auth_to_local", "RULE:[2:$1](hive)s/^.*$/hive/");
        //conf.set("hadoop.security.auth_to_local", "");
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab("hive/user2@HADOOP.COM", System.getProperty("hive_keytab"));
            System.out.printf("user: %s", UserGroupInformation.getCurrentUser());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        String JDBC_DB_URL = "jdbc:hive2://10.36.248.27:8719/default;principal=hive/quickstart.cloudera@HADOOP.COM";

        Connection con = DriverManager.getConnection(JDBC_DB_URL);
        System.out.println(con.getMetaData().getDriverVersion());
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
