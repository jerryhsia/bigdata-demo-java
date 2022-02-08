import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseWithKerberos {

    private static final String TABLE_NAME = "test:test";
    private static final String CF_DEFAULT = "cf";

    /**
     * HBase 通用客户端Kerberos认证
     * @param resources 配置文件资源
     * @param krb5Conf krb5.conf文件路径
     * @param principal Kerberos用户主体，eg：xingweidong@BIGDATA.ZXXK.COM
     * @param keytabFile keytab文件路径
     * @return
     * @throws IOException
     */
    public static Connection getHBaseConn(List<String> resources, String krb5Conf, String principal, String keytabFile) throws IOException {
        Configuration config = HBaseConfiguration.create();

        // 添加必要的配置文件 (hbase-site.xml, core-site.xml)
        for (int i = 0; i < resources.size(); i++) {
            config.addResource(new Path(resources.get(i)));
        }

        // Kerberos认证
        // 设置java安全krb5.conf
        System.setProperty("java.security.krb5.conf", krb5Conf);
        // 设置用户主体(Principal)
        config.set("kerberos.principal" , principal);
        // 使用用户keytab文件认证
        config.set("keytab.file" , keytabFile);

        UserGroupInformation.setConfiguration(config);
        try {
            // 登录
            UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 创建连接
        return ConnectionFactory.createConnection(config);
    }

    /**
     * HBase2.2.0+ 客户端Kerberos认证 (未验证)
     * @param resources 配置文件资源
     * @param krb5Conf krb5.conf文件路径
     * @param principal Kerberos用户主体，eg：xingweidong@BIGDATA.ZXXK.COM
     * @param keytabFile keytab文件路径
     * @return
     * @throws IOException
     */
    public static Connection getHBaseConn220(List<String> resources, String krb5Conf, String principal, String keytabFile) throws IOException {
        Configuration config = HBaseConfiguration.create();

        // 添加必要的配置文件 (hbase-site.xml, core-site.xml)
        for (int i = 0; i < resources.size(); i++) {
            config.addResource(new Path(resources.get(i)));
        }

        // 设置java安全krb5.conf （未验证是否需要此配置）
        System.setProperty("java.security.krb5.conf", krb5Conf);

        // Kerberos认证
        config.set("hbase.client.kerberos.principal", principal);
        config.set("hbase.client.keytab.file", keytabFile);

        // 创建连接
        return ConnectionFactory.createConnection(config);
    }

    public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    public static void createSchemaTables(Connection connection) throws IOException {
        try (Admin admin = connection.getAdmin()) {

            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.NONE));

            System.out.print("Creating table. ");
            createOrOverwrite(admin, table);
            System.out.println(" Done.");
        }
    }

    public static void main(String... args) throws IOException {
        // 添加必要的配置文件 (hbase-site.xml, core-site.xml)
        List<String> resources = new ArrayList<String>() {
            {
                add("/home/xwd/ws/hbase/hbase-clientconfig/hbase-conf/core-site.xml");
                add("/home/xwd/ws/hbase/hbase-clientconfig/hbase-conf/hbase-site.xml");
                add("/home/xwd/ws/hbase/hbase-clientconfig/hbase-conf/hdfs-site.xml");
            }
        };
        String krb5Conf = "/home/xwd/ws/hbase/krb5/krb5.conf";
        String principal = "xingweidong@BIGDATA.ZXXK.COM";
        String keytabFile = "/home/xwd/ws/hbase/krb5/xingweidong.keytab";

        // HBase操作
        Connection connection = getHBaseConn(resources, krb5Conf, principal, keytabFile);
        createSchemaTables(connection);

        System.out.println(" Put data ");
        Table table = connection.getTable(TableName.valueOf("test:test"));
        try {
            Put put = new Put(Bytes.toBytes("hbase_client_test"));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("col"), Bytes.toBytes("hbase_loginUserFromKeytab"));
            table.put(put);
        } finally {
            // 关闭连接
            table.close();
            connection.close();
        }
    }
}
