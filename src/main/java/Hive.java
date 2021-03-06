import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Hive {
    public static void main(String[] args) throws Exception {

        String connectionUrl = "jdbc:hive2://10.36.249.147:8719/default";
        String jdbcDriverName = "org.apache.hive.jdbc.HiveDriver";
        String sqlStatement = "show tables";

        Class.forName(jdbcDriverName);

        ProtectionDomain pd = org.apache.hive.jdbc.HiveDriver.class.getProtectionDomain();
        CodeSource cs = pd.getCodeSource();
        System.out.println(cs.getLocation());

        Connection con = DriverManager.getConnection(connectionUrl, "root", "");
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(sqlStatement);

        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }
}