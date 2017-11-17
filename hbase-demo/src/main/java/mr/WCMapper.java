package mr;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * <p/>
 * <li>Description:hbase整合MapReduce的mapper</li>
 * <li>@author: lee </li>
 * <li>Date: 2017/11/9 </li>
 * <li>@version: 1.0.0 </li>
 */
public class WCMapper extends TableMapper< Text, IntWritable > {

    @Override
    protected void map ( ImmutableBytesWritable key, Result value, Context context ) throws IOException, InterruptedException {

        String word = Bytes.toString( value.getValue( Bytes.toBytes( "cf1" ), Bytes.toBytes( "name" ) ) );
        context.write( new Text( word ),new IntWritable( 1 ) );
    }
}
