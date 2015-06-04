/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hdfs.hdfsadapter;


/**
 *
 * @author efi
 */
public class ReadXML {

    public static void main(final String[] args) throws Exception {
        /*Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new hdfs.hdfsadapter.XMLJob(), args);
        System.exit(res);*/
        
        String conf = "/home/efi/Utilities/hadoop/etc/hadoop/";
        HdfsFunctions f = new HdfsFunctions(conf);
        System.out.println(f.isLocalFile("file1"));
        System.exit(0);
    }

}
