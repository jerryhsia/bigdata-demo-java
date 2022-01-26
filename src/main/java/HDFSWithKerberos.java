import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class HDFSWithKerberos {
    static Configuration conf = new Configuration();

    static {
        System.setProperty("java.security.krb5.conf", "krb5.conf");//设置kerberos配置信息
        conf.set("fs.defaultFS", "hdfs://10.36.248.27:8722");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("fs.webhdfs.impl", org.apache.hadoop.hdfs.web.WebHdfsFileSystem.class.getName());
        conf.setBoolean("hadoop.security.authentication", true);
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab("hdfs/user@HADOOP.COM", "hdfs_user.keytab");//kerberos 认证
            UserGroupInformation.getLoginUser();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void getDirectoryFromHdfs(String direPath) {
        try {
            FileSystem fs = FileSystem.get(URI.create(direPath), conf);
            FileStatus[] filelist = fs.listStatus(new Path(direPath));
            for (int i = 0; i < filelist.length; i++) {
                System.out.println("_________" + direPath + "目录下所有文件______________");
                FileStatus fileStatus = filelist[i];
                System.out.println("Name:" + fileStatus.getPath().getName());
                System.out.println("Size:" + fileStatus.getLen());
                System.out.println("Path:" + fileStatus.getPath());
            }
            fs.close();
        } catch (Exception e) {

        }
    }

    public static void uploadFile(String src, String dst) throws IOException {

        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(src); //本地上传文件路径
        Path dstPath = new Path(dst); //hdfs目标路径
        //调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
        fs.copyFromLocalFile(false, srcPath, dstPath);

        //打印文件路径
        System.out.println("Upload to " + conf.get("fs.default.name"));
        System.out.println("------------list files------------" + "\n");
        FileStatus[] fileStatus = fs.listStatus(dstPath);
        for (FileStatus file : fileStatus) {
            System.out.println(file.getPath());
        }
        fs.close();
    }

    public static void main(String[] args) throws IOException {
        getDirectoryFromHdfs("/user/hdfs");
    }
}
