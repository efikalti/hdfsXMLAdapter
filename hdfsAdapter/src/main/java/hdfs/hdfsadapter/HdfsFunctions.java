/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hdfs.hdfsadapter;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import org.apache.hadoop.conf.Configuration;
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
     * Returns true if the file path exists or it is located somewhere in the home directory of the user that called the function.
     * Searches in subdirectories of the home directory too.
     * @param filename
     * @return 
     */
    public boolean isLocatedInHDFS(String filename) throws IOException
    {
        //search file path
        if (fs.exists(new Path(filename)))
        {
            return true;
        }
        //Search every file and folder in the home directory
        if (searchInDirectory(fs.getHomeDirectory(), filename) != null)
        {
            return true;
        }
        return false;
    }
    
    /**
     * Searches the given directory and subdirectories for the file.
     * @param directory to search
     * @param filename of file we want
     * @return path if file exists in this directory.else return null.
     */
    public Path searchInDirectory(Path directory, String filename)
    {
        //Search every folder in the directory
        try {
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(directory, true);
            String[] parts;
            Path path;
            while(it.hasNext())
            {
                path = it.next().getPath();
                parts = path.toString().split("/");
                if(parts[parts.length-1].equals(filename))
                {
                    return path;
                }
            }
        } catch (IOException ex) {
            System.err.println(ex);
        }
        return null;
    }
    
    public void readFile(String filename) throws IOException
    {
        Path path = this.searchInDirectory(fs.getHomeDirectory(), filename);
        if (path != null)
        {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            line = br.readLine();
            while (line != null) {
                System.out.println(line);
                line = br.readLine();
            }
            fs.close();
        }
    }
    
    /**
     * Check if file exists.
     * @param filename
     * @return 
     */
    public boolean isLocalFile(String filename)
    {
        return new File(filename).exists();
    }
}
