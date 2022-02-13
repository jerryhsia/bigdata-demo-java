import org.apache.hadoop.hbase.thrift2.generated.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class HBaseWithThrift2 {
    public static void main(String[] args) {
        try {
            //创建连接
            TTransport transport = new TSocket("10.36.249.147", 8720, 5000);
            TProtocol protocol = new TBinaryProtocol(transport);
            THBaseService.Iface client = new THBaseService.Client(protocol);
            transport.open();

            //指定表名
            ByteBuffer table = ByteBuffer.wrap("user".getBytes());
            //指定行键
            ByteBuffer row = ByteBuffer.wrap("xiajie01".getBytes());
            //指定列簇
            ByteBuffer family = ByteBuffer.wrap("detail".getBytes());
            //指定列名
            ByteBuffer qualifier = ByteBuffer.wrap("birthday".getBytes());

            //查询
            TGet get = new TGet();
            get.setRow(row);
            TColumn col = new TColumn()
                    .setFamily(family); //不指定列簇,即是获取所有列簇
                    // .setQualifier(qualifier);//不指定列名,即是获取所有列
            get.setColumns(Arrays.asList(col));
            TResult result = client.get(table, get);
            System.out.println(new String(result.getRow()));
            result.getColumnValues().forEach(c -> {
                System.out.println(new String(result.getRow()) + "  " + new String(c.getFamily()) + "_" + new String(c.getQualifier()) + ":" + new String(c.getValue()));
            });

            //批量查询
            TScan scan = new TScan();
            List<TResult> resultList = client.getScannerResults(table, scan, 2);
            resultList.forEach(r -> r.getColumnValues().forEach(c -> {
                System.out.println(new String(r.getRow()) + "  " + new String(c.getFamily()) + "_" + new String(c.getQualifier()) + ":" + new String(c.getValue()));
            }));

            //保存或更新
            TPut put = new TPut();
            put.setRow("row1".getBytes());
            TColumnValue colVal = new TColumnValue();  //定义属性值
            colVal.setFamily(family);
            colVal.setQualifier("name".getBytes());
            colVal.setValue("banana".getBytes());
            put.setColumnValues(Arrays.asList(colVal));
            client.put(table, put);

            //删除
            TDelete delete = new TDelete();
            delete.setRow("row1".getBytes());
            client.deleteSingle(table, delete);

            transport.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
