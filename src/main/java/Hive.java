import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Hive {
    public static void main(String[] args) throws Exception {

        String connectionUrl = "jdbc:hive2://10.36.249.147:8719/default";
        String jdbcDriverName = "org.apache.hive.jdbc.HiveDriver";
        String sqlStatement = "select count(*) from titanic";

        Class.forName(jdbcDriverName);
        try {
            Connection con = DriverManager.getConnection(connectionUrl, "root", "");
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