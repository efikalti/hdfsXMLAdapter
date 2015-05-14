/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hdfs.hdfsadapter;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author efi
 */
public class Main {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

        long startTime = System.currentTimeMillis();
        
        // Paths of input and output directory
        Path input = new Path("/user/efi/files/");    //input path
        Path output = new Path("/user/efi/xml");    //output path
        Path temp = new Path("/user/efi/buffer.txt");

        // Create configuration
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://localhost:9000");
        conf.set("tag", "</item>");
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

        Job read = new Job(conf, "Read from HDFS");
        read.setNumReduceTasks(0);
        // Assign Map and Reduce class
        read.setJarByClass(XmlReadMapper.class);
        read.setMapperClass(XmlReadMapper.class);
        // Define the data type of key and value
        read.setMapOutputKeyClass(Text.class); //key from map
        read.setMapOutputValueClass(Text.class);//value from map
        // Set input path 
        FileInputFormat.addInputPath(read, input);
        //read.setInputFormatClass(XmlInputFormat.class);
        //read.setInputFormatClass(XmlInputFormatBlockSolution.class);
        read.setInputFormatClass(XmlInputFormatOneBufferSolution.class);
        
        // Set output path
        FileOutputFormat.setOutputPath(read, output);
        read.setOutputFormatClass(TextOutputFormat.class);
        conf.setInt("current_block", 0);
        
        
        //Execute job 
        int code = read.waitForCompletion(true) ? 0 : 1;
        
        
        long endTime   = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println(totalTime);
    }

}
