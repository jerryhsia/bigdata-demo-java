import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import sun.security.krb5.KrbException;

public class KrbUtil {
    public static void initKrbHiveAuth1() {
        Configuration conf = new Configuration();
        System.setProperty("hive_keytab", "/Users/xiajie01/Develop/acg/bigdata-demo-java/hive_user2.keytab");
        System.setProperty("java.security.krb5.conf", "/Users/xiajie01/Develop/acg/bigdata-demo-java/krb5.conf");
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("hadoop.security.auth_to_local", "DEFAULT");
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab("hive/user2@HADOOP.COM", System.getProperty("hive_keytab"));
            System.out.printf("user: %s", UserGroupInformation.getCurrentUser());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void initKrbHiveAuth2() throws KrbException {
        System.setProperty("java.security.auth.login.config","hive-gss-jaas.conf");
        System.setProperty("sun.security.jgss.debug","true");
        System.setProperty("javax.security.auth.useSubjectCredsOnly","false");
        System.setProperty("java.security.krb5.conf","krb5.conf");
        sun.security.krb5.Config.refresh();
        System.out.println(sun.security.krb5.Config.getInstance().toString());
    }
}
