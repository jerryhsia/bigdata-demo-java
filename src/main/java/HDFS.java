import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * HDFS测试
 *
 * @author zhi
 * @since 2019年9月10日18:28:24
 *
 */
public class HDFS {
    private static FileSystem fileSystem = null;

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        // 一般为8022或9000端口，按需修改
        fileSystem = FileSystem.get(new URI("hdfs://10.36.249.147:8722"), configuration, "root");
        listFiles();
    }

    public static void mkdir() {
        try {
            boolean result = fileSystem.mkdirs(new Path("/test"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void listFiles() {
        try {
            FileStatus[] statuses = fileSystem.listStatus(new Path("/user"));
            for (FileStatus file : statuses) {
                System.out.println(file.getPath().getName());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}