package base;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * <p/>
 * <li>Description:phoniex操作hbase</li>
 * <li>@author: lee </li>
 * <li>Date: 2017/10/17 </li>
 * <li>@version: 1.0.0 </li>
 */
public class PhoniexTest {

    private Connection conn = null;

    private Statement stmt = null;

    /**
     * 加载驱动
     */
    static {
        try {
            Class.forName( "org.apache.phoenix.jdbc.PhoenixDriver" );
        } catch ( ClassNotFoundException e ) {
            e.printStackTrace();
        }
    }

    /**
     * 获取连接
     *
     * @return
     */
    public Connection getConnection () {

//        final String url = "jdbc:phoenix:hadoop-001,hadoop-002,hadoop-003:2181";
//        final String url_iwangding = "jdbc:phoenix:zookeeper1,zookeeper2,zookeeper3:2181";
        final String url_iwangding2 = "jdbc:phoenix:hadoop1,hadoop2,hadoop3:2181";
        if ( conn == null ) {
            try {
                // Phoenix DB不支持直接设置连接超时
                // 所以这里使用线程池的方式来控制数据库连接超时
                final ExecutorService exec = Executors.newFixedThreadPool( 1 );
                Callable< Connection > call = new Callable< Connection >() {

                    public Connection call () throws Exception {

                        return DriverManager.getConnection( url_iwangding2 );
                    }
                };
                Future< Connection > future = exec.submit( call );
                // 如果在3s钟之内，还没得到 Connection 对象，则认为连接超时，不继续阻塞，防止服务夯死
                conn = future.get( 3 * 60 * 1000, TimeUnit.MILLISECONDS );
                System.out.println( "================成功获取连接================" );
                exec.shutdownNow();
            } catch ( InterruptedException e ) {
                e.printStackTrace();
            } catch ( ExecutionException e ) {
                e.printStackTrace();
            } catch ( TimeoutException e ) {
                e.printStackTrace();
            }
        }
        return conn;
    }

    /**
     * 执行sql方法
     *
     * @param sql
     */
    public void exeSql ( String sql ) {

        conn = getConnection();
        try {
            stmt = conn.createStatement();
            stmt.execute( sql );
            conn.commit();
            System.out.println( "=================SQL执行成功================" );
        } catch ( SQLException e ) {
            e.printStackTrace();
        } finally {
            try {
                stmt.close();
                conn.close();
            } catch ( SQLException e ) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建表
     */
    public void createTable ( String sql ) {

        exeSql( sql );
        System.out.println( "=====================建表成功==================" );
    }

    /**
     * 插入数据
     */
    public void upsertData () {

        conn = getConnection();
        try {
            stmt = conn.createStatement();
            String[] index_id = { "cdn2x", "cdn4x", "cdn5x", "spDelay", "vodDelay", "tvodDelay", "tstvDelay" };
            String[] ip = { "123.147.112.250", "123.147.112.251", "123.147.112.25", "123.147.112.26", "123.147.112.27", "123.147.112.28", "123.147.112.29", "123.147.112.30", "123.147.112.31", "123.147.112.32" };
            String startTime = "2017-10-23 00:00:00";
            SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
            Date startDate = sdf.parse( startTime );
            Calendar ca = Calendar.getInstance();
            ca.setTime( startDate );
            long startTimeMillis = System.currentTimeMillis();
            long endTimeMillis = System.currentTimeMillis() + 10000;
            long upTimeMillis = endTimeMillis;
            long flowId = 1;
            for ( int i = 1; i < 1000000; i++ ) {
                if ( i % 100 == 0 ) {
                    ca.add( ca.SECOND, 10 );
                    startTime = sdf.format( ca.getTime() );
                }
                for ( int j = 0; j < 7; j++ ) {
                    String rowkey = UUID.randomUUID().toString().replaceAll( "-", "" );
                    String endTime = sdf.format( endTimeMillis );
                    String upTime = sdf.format( upTimeMillis );
                    String sql = "upsert into C_CDN_INDEX_MULTIPLE_CLOUMN values (" + ( flowId++ ) + "," + i + ",'" + i + "','" + i + "','" + i + "','" + i + "','" + index_id[j] + "','" + i + "','" + startTime + "','" + endTime + "','" + upTime + "',1,'" + ip[j] + "',0)";
                    System.out.println( sql );
                    stmt.executeUpdate( sql );
                    conn.commit();
                    System.out.println( "第" + ( flowId ) + "条插入成功" );
                }
            }
            long endUpsert = System.currentTimeMillis();
            long startUpsert = System.currentTimeMillis();
            long allTimes = endUpsert - startUpsert;
            System.out.println( "总共用时：" + allTimes + "ms" );
        } catch ( SQLException e ) {
            e.printStackTrace();
        } catch ( ParseException e ) {
            e.printStackTrace();
        } finally {
            try {
                stmt.close();
                conn.close();
            } catch ( SQLException e ) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 插入数据到client_info表
     */
    public void upsertData_client_info () {

        conn = getConnection();
        try {
            stmt = conn.createStatement();
            String[] index_id = { "cdn2x", "cdn4x", "cdn5x", "spDelay", "vodDelay", "tvodDelay", "tstvDelay" };
            String[] ip = { "123.147.112.250", "123.147.112.251", "123.147.112.25", "123.147.112.26", "123.147.112.27", "123.147.112.28", "123.147.112.29", "123.147.112.30", "123.147.112.31", "123.147.112.32" };
            String startTime = "2017-10-23 00:00:00";
            SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
            Date startDate = sdf.parse( startTime );
            Calendar ca = Calendar.getInstance();
            ca.setTime( startDate );
            long startTimeMillis = System.currentTimeMillis();
            long endTimeMillis = System.currentTimeMillis() + 10000;
            long upTimeMillis = endTimeMillis;
            long flowId = 1;
            for ( int i = 580000; i < 640000; i++ ) {
                if ( i % 100 == 0 ) {
                    ca.add( ca.SECOND, 10 );
                    startTime = sdf.format( ca.getTime() );
                }
                String endTime = sdf.format( endTimeMillis );
                String upTime = sdf.format( upTimeMillis );
                String sql = "upsert into client_info values (''," + ( flowId++ ) + ",'client_id','account_code','" + i + "','stp_ip','wlan_ip','getway'," + i + ",'mac','mac2','net_model','c_type','client_model','client_version','client_factory'," + i + "," + i + ",'client_cpu','os_type','os_version','province','city','area','addr','parent_node','" + upTime + "','bd_account','1','" + endTime + "','opt_name'," + i + ")";
                System.out.println( sql );
                stmt.executeUpdate( sql );
                conn.commit();
                System.out.println( "第" + ( flowId ) + "条插入成功" );
            }
            long endUpsert = System.currentTimeMillis();
            long startUpsert = System.currentTimeMillis();
            long allTimes = endUpsert - startUpsert;
            System.out.println( "总共用时：" + allTimes + "ms" );
        } catch ( SQLException e ) {
            e.printStackTrace();
        } catch ( ParseException e ) {
            e.printStackTrace();
        } finally {
            try {
                stmt.close();
                conn.close();
            } catch ( SQLException e ) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 批量插入
     */
    public void insertDataBatch () {

        PreparedStatement pstmt = null;
        conn = getConnection();
        long start = System.currentTimeMillis();
        try {
            conn.setAutoCommit( false );
            String sql = "upsert into C_CDN_INDEX_SALT values(?,?,?,?,?,?,?)";
            pstmt = conn.prepareStatement( sql );
            for ( int i = 0; i < 20000; i++ ) {
                String rowkey = UUID.randomUUID().toString().replaceAll( "-", "" );
                pstmt.setString( 1, rowkey + i );
                pstmt.setLong( 2, 33 + i );
                pstmt.setString( 3, "asfsdffds" );
                pstmt.setString( 4, "sdfggg" );
                pstmt.setString( 5, "sdfff" );
                pstmt.setString( 6, "safgfg" );
                pstmt.setString( 7, "sfdghjjjj" );
                if ( i % 1000 == 0 ) {
                    pstmt.executeBatch();
                }
            }
            pstmt.executeBatch();
            conn.commit();
            long end = System.currentTimeMillis();
            long tm = end - start;
            System.out.println( "总共使用时间" + tm );
        } catch ( SQLException e ) {
            e.printStackTrace();
        } finally {
            try {
                pstmt.close();
                conn.close();
            } catch ( SQLException e ) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 查询所有
     */
    public void findAll ( String sql ) {

        PreparedStatement pstmt = null;
        conn = getConnection();
        long start = System.currentTimeMillis();
        try {
            conn.setAutoCommit( false );
            pstmt = conn.prepareStatement( sql );
            ResultSet rset = pstmt.executeQuery( sql );
            while ( rset.next() ) {
                //WEB_STAT
//                System.out.println( rset.getString( 1 ) + "---" + rset.getString( 2 ) + "---" + rset.getString( 4 ) + "---" + rset.getString( 5 ) + "---" + rset.getString( 6 ) + "---" + rset.getString( 7 ) );
                System.out.println( rset.getString( 1 ) );
            }
            long end = System.currentTimeMillis();
            long tm = end - start;
            System.out.println( "总共使用时间" + tm );
        } catch ( SQLException e ) {
            e.printStackTrace();
        } finally {
            try {
                pstmt.close();
                conn.close();
            } catch ( SQLException e ) {
                e.printStackTrace();
            }
        }
    }

    public static void main ( String[] args ) {

        PhoniexTest phoniexTest = new PhoniexTest();
//        String sql = "select count(1)  test2";
//        String sql = "create table test1 (mykey integer not null primary key, mycolumn varchar)";
        phoniexTest.upsertData_client_info();

    }
}
