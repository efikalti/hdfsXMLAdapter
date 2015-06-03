/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hdfs.hdfsadapter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/**
 *
 * @author Efi Kaltirimidou
 */
public class HdfsFunctions {
    
    private final Configuration conf;
    private FileSystem fs;
    
    /**
     * Create the configuration and add the paths for core-site and hdfs-site as resources.
     * Initialize an instance a hdfs FileSystem for this configuration.
     * @param hadoop_conf_filepath 
     */
    public HdfsFunctions(String hadoop_conf_filepath)
    {
        this.conf = new Configuration();
        conf.addResource(new Path(hadoop_conf_filepath + "core-site.xml"));
        conf.addResource(new Path(hadoop_conf_filepath + "hdfs-site.xml"));
        
        try {
            fs =  FileSystem.get(conf);
        } catch (IOException ex) {
            System.err.println(ex);
        }
    }
    
    /**
     * Returns true if the file exists somewhere in the home directory of the user that called the function.
     * Searches in subdirectories too.
     * @param filename
     * @return 
     */
    public boolean isLocatedInHDFS(String filename)
    {
        //Search every file and folder in the home directory
        return searchInDirectory(fs.getHomeDirectory(), filename);
    }
    
    public boolean searchInDirectory(Path directory, String filename)
    {
        //Search every folder in the directory
        try {
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(directory, true);
            String[] parts;
            while(it.hasNext())
            {
                parts = it.next().getPath().toString().split("/");
                if(parts[parts.length-1].equals(filename))
                {
                    return true;
                }
            }
        } catch (IOException ex) {
            System.err.println(ex);
        }
        return false;
    }
    
    public void readFile(String filename) throws IOException
    {

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    System.out.println("Enter the file path...");
    String filePath = br.readLine();

    Path path = new Path(filePath);
    FSDataInputStream inputStream = fs.open(path);
    System.out.println(inputStream.available());
    fs.close();
    }
}
