import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ImpalaAsHiveKerberos {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        System.setProperty("hive_keytab", "/Users/xiajie01/Develop/acg/bigdata-demo-java/hive_user2.keytab");
        System.setProperty("java.security.krb5.conf", "/Users/xiajie01/Develop/acg/bigdata-demo-java/krb5.conf");
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab("hive/user2@HADOOP.COM", System.getProperty("hive_keytab"));
            System.out.printf("user: %s", UserGroupInformation.getCurrentUser());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        // 这个地址填写自己的impala server地址,默认端口为21050，请按需修改
        String connectionUrl = "jdbc:hive2://10.36.248.27:8703/default;AuthMech=1;principal=impala/quickstart.cloudera@HADOOP.COM";
        String jdbcDriverName = "org.apache.hive.jdbc.HiveDriver";
        String sqlStatement = "show tables";

        //加载驱动
        Class.forName(jdbcDriverName);
        try  {
            Connection con = DriverManager.getConnection(connectionUrl);
            //查询
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(sqlStatement);
            System.out.println("---begin query---");

            //打印输出
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
            System.out.println("---end query---");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}