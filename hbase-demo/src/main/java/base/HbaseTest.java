package base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * <p/>
 * <li>Description:</li>
 * <li>@author: lee </li>
 * <li>Date: 2017/10/15 </li>
 * <li>@version: 1.0.0 </li>
 */
public class HbaseTest {

    static Configuration conf = null;

    // Hbase连接
    private Connection conn = null;

    //数据库管理对象
    private HBaseAdmin admin = null;

    private TableName tableName = null;

    private Table table = null;

    // 初始化配置
    @Before
    public void init () throws Exception {

        conf = HBaseConfiguration.create();
        // 如果不设置zookeeper地址，可以将hbase-site.xml文件复制到resource目录下
        conf.set( "hbase.zookeeper.quorum", "hadoop-001,hadoop-002,hadoop-003" );// zookeeper 地址
        conf.set( "hbase.zookeeper.property.clientPort", "2188" );// zookeeper 客户端端口，默认为2188，可以不用设置
        conn = ConnectionFactory.createConnection( conf );// 创建连接
        // admin = new HBaseAdmin(conf); // 已弃用，不推荐使用
        admin = (HBaseAdmin) conn.getAdmin(); // hbase 表管理类
        tableName = TableName.valueOf( "students" ); // 表名
        table = conn.getTable( tableName );// 表对象
    }

    //---------------------------------------------------DDL 操作 Start--------------------------------------------------//
    // 创建表 HTableDescriptor、HColumnDescriptor、addFamily()、createTable()
    @Test
    public void createTable () throws Exception {

        if ( admin.tableExists( tableName ) ) {
            System.out.println( "Warning : the table is existed !" );
        } else {
            // 创建表描述类
            HTableDescriptor desc = new HTableDescriptor( tableName );
            // 添加列族info
            HColumnDescriptor family_info = new HColumnDescriptor( "info" );
            desc.addFamily( family_info );
            // 添加列族address
            HColumnDescriptor family_address = new HColumnDescriptor( "address" );
            desc.addFamily( family_address );
            // 创建表
            admin.createTable( desc );
            System.out.println( "create table success!" );
        }
    }

    // 删除表 先弃用表disableTable(表名)，再删除表 deleteTable(表名)
    @Test
    public void deleteTable () throws Exception {

        admin.disableTable( tableName );
        admin.deleteTable( tableName );
    }

    // 添加列族 addColumn(表名,列族)
    @Test
    public void addFamily () throws Exception {

        admin.addColumn( tableName, new HColumnDescriptor( "hobbies" ) );
    }

    // 删除列族 deleteColumn(表名,列族)
    @Test
    public void deleteFamily () throws Exception {

        admin.deleteColumn( tableName, Bytes.toBytes( "hobbies" ) );
    }

    // --------------------DDL 操作 End---------------------

    // ----------------------DML 操作 Start-----------------
    // 添加数据 Put(列族,列,列值)（HBase 中没有修改，插入时rowkey相同，数据会覆盖）
    @Test
    public void insertData () throws Exception {
        // 添加一条记录
        // Put put = new Put(Bytes.toBytes("1001"));
        // put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("San-Qiang Zhang"));
        // put.addColumn(Bytes.toBytes("address"), Bytes.toBytes("province"), Bytes.toBytes("Hebei"));
        // put.addColumn(Bytes.toBytes("address"), Bytes.toBytes("city"), Bytes.toBytes("Shijiazhuang"));
        // table.put(put);

        // 添加多条记录（批量插入）
        List< Put > putList = new ArrayList< Put >();
        Put put1 = new Put( Bytes.toBytes( "1002" ) );
        put1.addColumn( Bytes.toBytes( "info" ), Bytes.toBytes( "name" ), Bytes.toBytes( "Lisi" ) );
        put1.addColumn( Bytes.toBytes( "info" ), Bytes.toBytes( "sex" ), Bytes.toBytes( "1" ) );
        put1.addColumn( Bytes.toBytes( "address" ), Bytes.toBytes( "city" ), Bytes.toBytes( "Shanghai" ) );
        Put put2 = new Put( Bytes.toBytes( "1003" ) );
        put2.addColumn( Bytes.toBytes( "info" ), Bytes.toBytes( "name" ), Bytes.toBytes( "Lili" ) );
        put2.addColumn( Bytes.toBytes( "info" ), Bytes.toBytes( "sex" ), Bytes.toBytes( "0" ) );
        put2.addColumn( Bytes.toBytes( "address" ), Bytes.toBytes( "city" ), Bytes.toBytes( "Beijing" ) );
        Put put3 = new Put( Bytes.toBytes( "1004" ) );
        put3.addColumn( Bytes.toBytes( "info" ), Bytes.toBytes( "name_a" ), Bytes.toBytes( "Zhaosi" ) );
        Put put4 = new Put( Bytes.toBytes( "1004" ) );
        put4.addColumn( Bytes.toBytes( "info" ), Bytes.toBytes( "name_b" ), Bytes.toBytes( "Wangwu" ) );
        putList.add( put1 );
        putList.add( put2 );
        putList.add( put3 );
        putList.add( put4 );
        table.put( putList );
    }

    // 删除数据 Delete
    @Test
    public void deleteData () throws Exception {
//        删除一条数据（行健为1002）
//        Delete delete = new Delete(Bytes.toBytes("1002"));
//        table.delete(delete);

        // 删除行健为1003，列族为info的数据
        // Delete delete = new Delete(Bytes.toBytes("1003"));
        // delete.addFamily(Bytes.toBytes("info"));
        // table.delete(delete);

        // 删除行健为1，列族为address，列为city的数据
//        Delete delete = new Delete( Bytes.toBytes( "1001" ) );
//        delete.addColumn( Bytes.toBytes( "address" ), Bytes.toBytes( "city" ) );
//        table.delete( delete );
    }

    // 单条查询 Get
    @Test
    public void getData () throws Exception {

        Get get = new Get( Bytes.toBytes( "1001" ) );
        // get.addFamily(Bytes.toBytes("info")); //指定获取某个列族
        // get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name")); //指定获取某个列族中的某个列
        Result result = table.get( get );

        System.out.println( "行健：" + Bytes.toString( result.getRow() ) );
        byte[] name = result.getValue( Bytes.toBytes( "info" ), Bytes.toBytes( "name" ) );
        byte[] sex = result.getValue( Bytes.toBytes( "info" ), Bytes.toBytes( "sex" ) );
        byte[] city = result.getValue( Bytes.toBytes( "address" ), Bytes.toBytes( "city" ) );
        byte[] province = result.getValue( Bytes.toBytes( "address" ), Bytes.toBytes( "province" ) );
        if ( name != null ) System.out.println( "姓名：" + Bytes.toString( name ) );
        if ( sex != null ) System.out.println( "性别：" + Bytes.toString( sex ) );
        if ( province != null ) System.out.println( "省份：" + Bytes.toString( province ) );
        if ( city != null ) System.out.println( "城市：" + Bytes.toString( city ) );
    }

    // 全表扫描 Scan
    @Test
    public void scanData () throws Exception {

        Scan scan = new Scan(); // Scan 全表扫描对象
        // 行健是以字典序排序，可以使用scan.setStartRow()，scan.setStopRow()设置行健的字典序
        // scan.addFamily(Bytes.toBytes("info")); // 只查询列族info
        //scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name")); // 只查询列name
        ResultScanner scanner = table.getScanner( scan );
        printResult1( scanner );
    }

    // 全表扫描：列值过滤器（过滤列植的相等、不等、范围等） SingleColumnValueFilter
    @Test
    public void singleColumnValueFilter () throws Exception {
        /**
         * CompareOp 是一个枚举，有如下几个值
         * LESS                 小于
         * LESS_OR_EQUAL        小于或等于
         * EQUAL                等于
         * NOT_EQUAL            不等于
         * GREATER_OR_EQUAL     大于或等于
         * GREATER              大于
         * NO_OP                无操作
         */
        // 查询列名大于San-Qiang Zhang的数据
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes( "info" ), Bytes.toBytes( "name" ),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes( "San-Qiang Zhang" ) );
        Scan scan = new Scan();
        scan.setFilter( singleColumnValueFilter );
        ResultScanner scanner = table.getScanner( scan );
        printResult1( scanner );
    }

    // 全表扫描：列名前缀过滤器（过滤指定前缀的列名） ColumnPrefixFilter
    @Test
    public void columnPrefixFilter () throws Exception {
        // 查询列以name_开头的数据
        ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter( Bytes.toBytes( "name_" ) );
        Scan scan = new Scan();
        scan.setFilter( columnPrefixFilter );
        ResultScanner scanner = table.getScanner( scan );
        printResult2( scanner );
    }

    // 全表扫描：多个列名前缀过滤器（过滤多个指定前缀的列名） MultipleColumnPrefixFilter
    @Test
    public void multipleColumnPrefixFilter () throws Exception {
        // 查询列以name_或c开头的数据
        byte[][] bytes = new byte[][]{ Bytes.toBytes( "name_" ), Bytes.toBytes( "c" ) };
        MultipleColumnPrefixFilter multipleColumnPrefixFilter = new MultipleColumnPrefixFilter( bytes );
        Scan scan = new Scan();
        scan.setFilter( multipleColumnPrefixFilter );
        ResultScanner scanner = table.getScanner( scan );
        printResult1( scanner );
    }

    // rowKey过滤器（通过正则，过滤rowKey值） RowFilter
    @Test
    public void rowFilter () throws Exception {
        // 匹配rowkey以100开头的数据
        // Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^100"));
        // 匹配rowkey以2结尾的数据
        RowFilter filter = new RowFilter( CompareFilter.CompareOp.EQUAL, new RegexStringComparator( "2$" ) );
        Scan scan = new Scan();
        scan.setFilter( filter );
        ResultScanner scanner = table.getScanner( scan );
        printResult1( scanner );
    }

    // 多个过滤器一起使用
    @Test
    public void multiFilterTest () throws Exception {
        /**
         * Operator 为枚举类型，有两个值 MUST_PASS_ALL 表示 and，MUST_PASS_ONE 表示 or
         */
        FilterList filterList = new FilterList( FilterList.Operator.MUST_PASS_ALL );
        // 查询性别为0（nv）且 行健以10开头的数据
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes( "info" ), Bytes.toBytes( "sex" ),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes( "0" ) );
        RowFilter rowFilter = new RowFilter( CompareFilter.CompareOp.EQUAL, new RegexStringComparator( "^10" ) );
        filterList.addFilter( singleColumnValueFilter );
        filterList.addFilter( rowFilter );
        Scan scan = new Scan();
        scan.setFilter( rowFilter );
        ResultScanner scanner = table.getScanner( scan );
        // printResult1(scanner);
        printResult2( scanner );
    }

    //**************************DML 操作 End***************************//

    /**
     * 打印查询结果：方法一
     */
    public void printResult1 ( ResultScanner scanner ) throws Exception {

        for ( Result result : scanner ) {
            System.out.println( "行健：" + Bytes.toString( result.getRow() ) );
            byte[] name = result.getValue( Bytes.toBytes( "info" ), Bytes.toBytes( "name" ) );
            byte[] sex = result.getValue( Bytes.toBytes( "info" ), Bytes.toBytes( "sex" ) );
            byte[] city = result.getValue( Bytes.toBytes( "address" ), Bytes.toBytes( "city" ) );
            byte[] province = result.getValue( Bytes.toBytes( "address" ), Bytes.toBytes( "province" ) );
            if ( name != null ) System.out.println( "姓名：" + Bytes.toString( name ) );
            if ( sex != null ) System.out.println( "性别：" + Bytes.toString( sex ) );
            if ( province != null ) System.out.println( "省份：" + Bytes.toString( province ) );
            if ( city != null ) System.out.println( "城市：" + Bytes.toString( city ) );
            System.out.println( "------------------------------" );
        }
    }

    /**
     * 打印查询结果：方法二
     */
    public void printResult2 ( ResultScanner scanner ) throws Exception {

        for ( Result result : scanner ) {
            System.out.println( "-----------------------" );
            // 遍历所有的列及列值
            for ( Cell cell : result.listCells() ) {
                System.out.print( Bytes.toString( CellUtil.cloneQualifier( cell ) ) + "：" );
                System.out.print( Bytes.toString( CellUtil.cloneValue( cell ) ) + "\t" );
            }
            System.out.println();
            System.out.println( "-----------------------" );
        }
    }

    // 释放资源
    @After
    public void destory () throws Exception {

        admin.close();
    }
}
