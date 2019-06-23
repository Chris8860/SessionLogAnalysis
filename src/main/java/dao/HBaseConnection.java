package dao;

import javafx.scene.control.Tab;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

import static utils.WebLogConstants.zkConnect;

/**
 * @ClassName: HBaseConnection
 * @Description: 获取HBase连接
 * @Author: chris
 * @Date: 2019/6/23
 */
public class HBaseConnection {
    private final static HBaseConnection INSTANCE =  new HBaseConnection();
    private static Configuration conf;
    private static Connection connection;

    /**
     * @Description: 默认的构造方法，给自己调用，作用是传入conf配置
     * @Param:
     * @return:
     * @Author: Chris
     * @Date: 2019/6/23
     */
    private HBaseConnection() {
        try {
            if (conf == null){
                conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum", zkConnect);
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * @Description: 获取HBase的连接对象
     * @Param:
     * @return: org.apache.hadoop.hbase.client.Connection
     * @Author: Chris
     * @Date: 2019/6/23
     */
    private Connection getConnection(){
        if(connection == null || connection.isClosed()){
            try {
                connection = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    /**
     * @Description: 暴露给外部使用的获取HBase连接的方法
     * @Param:
     * @return: org.apache.hadoop.hbase.client.Connection
     * @Author: Chris
     * @Date: 2019/6/23
     */
    public static Connection getHBaseConnection(){
        return INSTANCE.getConnection();
    }

    /**
     * @Description: 获取到访问一个表的表实现。
     * @Param: [tableName : 表名]
     * @return: org.apache.hadoop.hbase.client.Table
     * @Author: Chris
     * @Date: 2019/6/23
     */
    public static Table getTable(String tableName) throws IOException {
            return INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
    }


    /**
     * @Description: 关闭链接
     * @Param:
     * @return: void
     * @Author: Chris
     * @Date: 2019/6/23 0023
     */
    public static void closeConn() throws IOException {
        if(connection != null){
            if(!connection.isClosed()){
                connection.close();
            }
        }
    }
}
