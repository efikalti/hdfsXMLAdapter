package hdfs.hdfsadapter;


import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Efi Kaltirimidou
 */
public class XmlReadMapper extends
        Mapper<Object, Text, Object, Text>{
    
     @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        context.write(key, value);
    }
    
}
