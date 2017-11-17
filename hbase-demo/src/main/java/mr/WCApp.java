package mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * <p/>
 * <li>Description:</li>
 * <li>@author: lee </li>
 * <li>Date: 2017/11/9 </li>
 * <li>@version: 1.0.0 </li>
 */
public class WCApp {

    public static void main ( String[] args ) {


        if ( args.length != 1 ) {
            System.err.println( "=================" );
            System.exit( -1 );
        }
        try {
            Job job = Job.getInstance();
            Configuration configuration = job.getConfiguration();
            configuration.set( "hbase.zookeeper.quorum","hadoop-001:2181,hadoop-002:2181,hadoop-003:2181" );
            FileSystem fileSystem = FileSystem.get( configuration );
            fileSystem.delete( new Path( args[1] ), true );
            job.setJarByClass( WCApp.class );
            //job名称
            job.setJobName( "word count" );
            FileOutputFormat.setOutputPath( job, new Path( args[0] ) );
            Scan scan = new Scan();
            TableMapReduceUtil.initTableMapperJob( "namespace1:mr", scan, WCMapper.class, Text.class, IntWritable.class, job );
            //设置表名
            job.getConfiguration().set( TableInputFormat.INPUT_TABLE, "namespace1:mr" );
            //设置MR类
            job.setMapperClass( WCMapper.class );
            job.setReducerClass( WCReducer.class );
            //设置输出格式
            job.setOutputKeyClass( Text.class );
            job.setOutputValueClass( IntWritable.class );
            //开始执行job
            System.exit( job.waitForCompletion( true ) ? 1 : 0 );
        } catch ( Exception e ) {
            e.printStackTrace();
        }
    }
}
