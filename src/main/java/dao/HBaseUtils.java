package dao;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.util.Arrays;

/**
 * @ClassName: HBaseUtils
 * @Description: HBase的Java API工具类
 * @Author: chris
 * @Date: 2019/6/23
 */
public class HBaseUtils {

    /**
     * @Description: 在HBase中创建一个table，根据参数指定其预先创建的列簇
     * @Param: [tableName:表名, colFams:列簇名列表]
     * @return: boolean
     * @Author: Chris
     * @Date: 2019/6/23
     */
    public static boolean createTable(String tableName, String[] colFams) {
        try(HBaseAdmin admin = (HBaseAdmin) HBaseConnection.getHBaseConnection().getAdmin()){
            if(admin.tableExists(tableName))
                return false;
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

//            Arrays.stream(colFams).forEach(colFam -> {
//                HColumnDescriptor columnDescriptor = new HColumnDescriptor(colFam);
//                columnDescriptor.setMaxVersions(1);
//                tableDescriptor.addFamily(columnDescriptor); //添加列簇
//            });
            for(String colFam : colFams){
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(colFam);
                columnDescriptor.setMaxVersions(1);
                tableDescriptor.addFamily(columnDescriptor); //添加列簇
            }
            admin.createTable(tableDescriptor);  //创建表
        }catch (IOException e){
            e.printStackTrace();
        }
        return true;
    }
    

}
