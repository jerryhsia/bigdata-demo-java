import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ImpalaAsHive {
    public static void main(String[] args) throws Exception {
        // 这个地址填写自己的impala server地址,默认端口为21050，请按需修改
        String connectionUrl = "jdbc:hive2://10.36.249.147:8703/default;auth=noSasl";
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