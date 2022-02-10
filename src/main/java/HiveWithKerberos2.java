import org.apache.hadoop.security.UserGroupInformation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
public class HiveWithKerberos2 {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws Exception {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        System.setProperty("java.security.auth.login.config","hive-gss-jaas.conf");
        System.setProperty("sun.security.jgss.debug","true");
        System.setProperty("javax.security.auth.useSubjectCredsOnly","false");
        System.setProperty("java.security.krb5.conf","krb5.conf");

        String JDBC_DB_URL = "jdbc:hive2://10.36.248.27:8719/default;principal=hive/quickstart.cloudera@HADOOP.COM";

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
