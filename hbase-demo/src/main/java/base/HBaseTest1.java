package base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * <p/>
 * <li>Description:</li>
 * <li>@author: lee </li>
 * <li>Date: 2017/10/16 </li>
 * <li>@version: 1.0.0 </li>
 */
public class HBaseTest1 {

    // Hbase连接
    private static Connection conn;

    // 数据库管理对象
    private static Admin admin;

    /**
     * 获取hbase连接
     */
    public static void getConn () {

        // 获取配置参数对象
        Configuration conf = HBaseConfiguration.create();
        //设置连接参数，集群ip
//        conf.set( "hbase.zookeeper.quorum", "hadoop-001,hadoop-002,hadoop-003" );
        conf.set( "hbase.zookeeper.quorum", "zookeeper1,zookeeper2,zookeeper3" );
        //设置连接参数，端口
        conf.set( "hbase.zookeeper.property.clientPort", "2181" );
        //获取连接
        try {
            conn = ConnectionFactory.createConnection( conf );
            //获取admin
            admin = conn.getAdmin();
        } catch ( IOException e ) {
            e.printStackTrace();
        }
        if ( admin == null ) {
            System.out.println( "=================连接失败=================" );
            System.out.println();
        } else {
            System.out.println( "=================连接成功=================" );
            System.out.println();
        }
    }

    /**
     * 关闭连接
     *
     * @throws IOException
     */
    public static void closeConnection () throws IOException {

        admin.close();
        System.out.println();
        System.out.println( "=================关闭连接=================" );
    }

    /**
     * 创建表
     *
     * @param tableName
     * @param familyColumnsName
     *
     * @throws IOException
     */
    public static void createHbaseTable ( String tableName, String[] familyColumnsName ) throws IOException {
        //create table name object
        TableName tableNameObject = TableName.valueOf( tableName );
        //judge if the table is existed
        if ( admin.tableExists( tableNameObject ) ) {
            System.out.println( "表已存在！！!" );
        } else {
            // table is not existed, create it
            // create table description object
            HTableDescriptor hTableDescriptor = new HTableDescriptor( tableNameObject );
            // create  familyColumns description object
            for ( String familyColumnName : familyColumnsName ) {
                // create object
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor( familyColumnName );
                // add familyColumn to tableDescriptor
                hTableDescriptor.addFamily( hColumnDescriptor );
            }
            // create table
            admin.createTable( hTableDescriptor );
            System.out.println( "建表成功!" );
        }
    }

    /**
     * 对表增加列族
     *
     * @param tableName
     * @param familyColumnName
     *
     * @throws IOException
     */
    public static void addFamilyColumn ( String tableName, String familyColumnName ) throws IOException {

        // get table object
        TableName tableObject = TableName.valueOf( tableName );
        // create familyColumn object
        HColumnDescriptor columnDescriptor = new HColumnDescriptor( familyColumnName );
        // add familyColumn to table
        admin.addColumn( tableObject, columnDescriptor );
    }

    /**
     * 删除列族
     *
     * @param tableName
     * @param familyColumnName
     *
     * @throws IOException
     * @author JainfeiZhang
     */
    public static void deleteFamilyColumn ( String tableName, String familyColumnName ) throws IOException {

        // get table object
        TableName tableObject = TableName.valueOf( tableName );
        // delete familyColumn
        admin.deleteColumn( tableObject, familyColumnName.getBytes() );
    }

    /**
     * 增加数据
     *
     * @param tableName        表名
     * @param familyColumnName 列族名
     * @param qualifierName
     * @param dataArray
     *
     * @throws IOException
     * @author lee
     */
    public static void insertData ( String tableName, String rowKeyName, String familyColumnName, String qualifierName, String[] dataArray ) throws IOException {

        // get table object
        Table table = conn.getTable( TableName.valueOf( tableName ) );
        // get put data object
        Put put;
        // create data put list
        List< Put > putList = new ArrayList< Put >();
        for ( String data : dataArray ) {
            put = new Put( Bytes.toBytes( rowKeyName ) );
            put.addColumn( familyColumnName.getBytes(), qualifierName.getBytes(), data.getBytes() );
            putList.add( put );
        }
        // put data in hbase table
        table.put( putList );
        System.out.println( "insert data success..." );
    }

    /**
     * 获取所有的列族名
     *
     * @param tableName
     *
     * @throws IOException
     */
    public static void getfamilyColumnsNames ( String tableName ) throws IOException {

        // get table object
        Table table = conn.getTable( TableName.valueOf( tableName ) );
        // get all familyColumns
        HColumnDescriptor[] hColumnDescriptor = table.getTableDescriptor().getColumnFamilies();
        for ( HColumnDescriptor hc : hColumnDescriptor ) {
            String familyColumnName = hc.getNameAsString();
            System.out.println( familyColumnName );
        }
    }

    /**
     * 遍历获取指定表、行键、列族、列的数据
     * 结果示例 ： mall/infoa:pv/1494196479710/Put/vlen=7/seqid=0 ，rowKey/family:qualifier/timestamp/...
     *
     * @param tableName
     * @param rowKeyName
     * @param familyName
     * @param qualifierName
     *
     * @throws IOException
     * @author JainfeiZhang
     */
    public static void getAllDatas ( String tableName, String rowKeyName, String familyName, String qualifierName ) throws IOException {

        // get table object
        Table table = conn.getTable( TableName.valueOf( tableName ) );
        // 获取rowkey
        Get get = new Get( Bytes.toBytes( rowKeyName ) );
        // 添加需要查询的列族和列名
        get.addColumn( Bytes.toBytes( familyName ), Bytes.toBytes( qualifierName ) );
        // 获取结果集
        Result result = table.get( get );
        // 遍历结果集
        if ( result.isEmpty() ) {
            System.out.println( " The result is Empty ! " );
        } else {
            // 获取游标
            CellScanner cellScanner = result.cellScanner();
            // 遍历
            while ( cellScanner.advance() ) {
                System.out.println( "Get cell : " + cellScanner.current() );
            }
        }
    }

    /**
     * 根据行键值按行获取数据
     *
     * @param tableName
     * @param rowKeyName
     *
     * @throws IOException
     */
    public static void getAllDateByRowKey ( String tableName, String rowKeyName, String columnFamilyName ) throws IOException {

        // get hbase table object
        Table table = conn.getTable( TableName.valueOf( tableName ) );
        // create query object
        Get get = new Get( Bytes.toBytes( rowKeyName ) );
        // query by rowKey
        Result result = table.get( get );
        byte[] row = result.getRow();
        System.out.println( "row key is:" + new String( row ) );
        for ( Cell cell : result.rawCells() ) {
            String rowKey = Bytes.toString( result.getRow() );
            String family = Bytes.toString( CellUtil.cloneFamily( cell ) );
            String qualifier = Bytes.toString( CellUtil.cloneQualifier( cell ) );
            String value = Bytes.toString( CellUtil.cloneValue( cell ) );
            if ( columnFamilyName.equals( columnFamilyName ) ) {
                System.out.println( "{\"rowkey\":\"" + rowKey +
                        "\",\"family\":\"" + family +
                        "\",\"qualifier\":\"" + qualifier +
                        "\",\"value\":\"" + value +
                        "\"}" );
            }
        }
    }

    /**
     * 浏览全表数据
     *
     * @param tableName
     *
     * @throws Exception
     */
    public static void scanData ( String tableName ) throws Exception {

        TableName tableNameObject = TableName.valueOf( tableName );
        long start = System.currentTimeMillis();
        Table table = conn.getTable( tableNameObject );
        Scan s = new Scan();
        ResultScanner rs = table.getScanner( s );
        //遍历打印表中的数据
        for ( Result r : rs ) {
            NavigableMap< byte[], NavigableMap< byte[], NavigableMap< Long, byte[] > > > navigableMap = r.getMap();
            for ( Map.Entry< byte[], NavigableMap< byte[], NavigableMap< Long, byte[] > > > entry : navigableMap.entrySet() ) {
                System.out.print( "【RowKey:" + Bytes.toString( r.getRow() ) );
                System.out.print( "】【familyColumnName:" + Bytes.toString( entry.getKey() ) + "】【" );
                NavigableMap< byte[], NavigableMap< Long, byte[] > > map = entry.getValue();
                for ( Map.Entry< byte[], NavigableMap< Long, byte[] > > en : map.entrySet() ) {
                    System.out.print( "qualifierName:" + Bytes.toString( en.getKey() ) );
                    NavigableMap< Long, byte[] > ma = en.getValue();
                    for ( Map.Entry< Long, byte[] > e : ma.entrySet() ) {
                        System.out.print( "】【TimeStamp:" + e.getKey() );
                        System.out.println( "】【dataArray:" + Bytes.toString( e.getValue() ) + "】" );
                    }
                }
            }
            System.out.println();
        }
        long end = System.currentTimeMillis();
        long tm = end - start;
        System.out.println( "总共使用时间" + tm+"ms" );
    }

    public static void main ( String[] args ) throws Exception {

        //获取连接
        getConn();
        String tableName = "c_cdn_index";
        String[] familyColumnsName = { "name", "age", "salary" };
        //建表
//        createHbaseTable( tableName, familyColumnsName );
        //获取列族名
//        getfamilyColumnsNames("WEB_STAT");
        //扫描全表
        scanData( tableName );
//String tableName, String rowKeyName, String familyColumnName, String qualifierName, String dataArray
        //增加数据
        String[] dataArray = { "1", "2", "3", "4" };
//        insertData( tableName,"1","age","unknow",dataArray );


        closeConnection();
    }
}
