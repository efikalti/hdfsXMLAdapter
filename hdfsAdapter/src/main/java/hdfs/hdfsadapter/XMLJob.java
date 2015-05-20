/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hdfs.hdfsadapter;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 *
 * @author efi
 */
public class XMLJob extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {
        
        long startTime = System.currentTimeMillis();
        
        // Paths of input and output directory
        Path input = new Path(args[0]);    //input path
        Path output = new Path(args[1]);    //output path
        Path temp = new Path("buffer.txt");

        
        // Create configuration
        Configuration conf = super.getConf();
        //conf.set("mapred.map.tasks", );
        conf.set("fs.default.name", "hdfs://localhost:9000");
        String tag = args[2];
        conf.set("start_tag", "<"+tag+">");
        conf.set("end_tag", "</"+tag+">");
        // Create connector with the hdfs system
        FileSystem hdfs = FileSystem.get(conf);

        // Delete output if it exists to avoid error
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }if (hdfs.exists(temp)) {
            hdfs.delete(output, true);
        }
        hdfs.createNewFile(temp);
        DistributedCache.addCacheFile(new URI("buffer.txt"), conf); 

        
        Job read = new Job(super.getConf(), "Read from HDFS");
        read.setNumReduceTasks(0);
        // Assign Map and Reduce class
        read.setJarByClass(XmlReadMapper.class);
        read.setMapperClass(XmlReadMapper.class);
        // Define the data type of key and value
        read.setMapOutputKeyClass(Text.class); //key from map
        read.setMapOutputValueClass(Text.class);//value from map
        // Set input path 
        FileInputFormat.addInputPath(read, input);
        
        //How to read each block
        //1.Whole Block
        //read.setInputFormatClass(XmlInputFormatBlockSolution.class);
        
        //2.One Buffer 
        //read.setInputFormatClass(XmlInputFormatOneBufferSolution.class);
        
        //3.Two buffers
        read.setInputFormatClass(XmlInputFormatTwoBufferSolution.class);
        
        // Set output path
        FileOutputFormat.setOutputPath(read, output);
        read.setOutputFormatClass(TextOutputFormat.class);
        
        //Execute job 
        int code = read.waitForCompletion(true) ? 0 : 1;
        
        URI[] filenames = DistributedCache.getCacheFiles(conf);
        
        long endTime   = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println(totalTime);
        
        return code;
    }
    
}
