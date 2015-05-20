package hdfs.hdfsadapter;
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.io.Closeables;
import java.io.BufferedWriter;
import java.io.FileWriter;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;

/**
 * Reads records that are delimited by a specific begin/end end_tag.
 */
public class XmlInputFormatTwoBufferSolution extends TextInputFormat {

  public static String END_TAG;
  public static String START_TAG;

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
    try {
        END_TAG = context.getConfiguration().get("end_tag");
        START_TAG = context.getConfiguration().get("start_tag");
      return new XmlRecordReader((FileSplit) split, context.getConfiguration());
    } catch (IOException ioe) {
      return null;
    }
  }

  /**
   * XMLRecordReader class to read through a given xml document to output xml blocks as records as specified
 by the start end_tag and end end_tag
   * 
   */
  public static class XmlRecordReader extends RecordReader<LongWritable, Text> {

    private final byte[] end_tag;
    private final byte[] start_tag;
    private final long start;
    private final long end;
    private int current_block = 0;
    private final FSDataInputStream fsin;
    private final DataOutputBuffer buffer = new DataOutputBuffer();
    private LongWritable currentKey;
    private Text currentValue;
    private final BlockLocation[] blocks;
    private final Configuration conf;
    private final String filename;
    

    public XmlRecordReader(FileSplit split, Configuration conf) throws IOException {
      end_tag = END_TAG.getBytes(Charsets.UTF_8);
      start_tag = START_TAG.getBytes(Charsets.UTF_8);
      
      // open the file and seek to the start of the split
      start = split.getStart();
      end = start + split.getLength();
      Path file = split.getPath();
      FileSystem fs = file.getFileSystem(conf);
      FileStatus fStatus = fs.getFileStatus(file);
      blocks = fs.getFileBlockLocations(fStatus, 0, fStatus.getLen());
      fsin = fs.open(split.getPath());
      fsin.seek(start);
      this.conf = conf;
      filename = split.getPath().getName();
    }

    /**
     * Read next block, sending the incomplete items from the end of block to 
 the a specified file out distributed cache
     * @param key
     * @param value
     * @return
     * @throws IOException 
     */
    private boolean next(LongWritable key, Text value) throws IOException {
        current_block = nextBlock();
      if (fsin.getPos() < end && current_block < blocks.length && readUntilMatch(start_tag,false)) {
        try {
          buffer.write(start_tag);
          if (readBlock()) {
            key.set(fsin.getPos());
            value.set(buffer.getData(), 0, buffer.getLength());
            return true;
          }
        } finally {
          buffer.reset();
        }
      }
      return false;
    }

    @Override
    public void close() throws IOException {
      Closeables.close(fsin, true);
    }

    @Override
    public float getProgress() throws IOException {
      return (fsin.getPos() - start) / (float) (end - start);
    }

    /**
     * Read from block(s) until you reach the end of file or find a matching bytes with match[]
     * @param match
     * @param withinBlock
     * @return
     * @throws IOException 
    */
    private boolean readBlock() throws IOException
    {
      boolean first = true;
      long item_size = 0;
      long pos = fsin.getPos();
      int b;
      while (true) {
        b = fsin.read();
        // end of file:
        if (b == -1) {
          return false;
        }
        
        readUntilMatch(end_tag, true);
        if (first)
        {
            item_size = fsin.getPos() - pos;
            first = false;
        }
        
        if (fsin.getPos() >= end)
        {
            return true;
        }
        
        // There isnt another complete item until the end of the block
        if ((end - fsin.getPos()) < item_size)
        {
            URI[] filenames = DistributedCache.getCacheFiles(conf);
            writeToFile(filenames[0].toString());
            return true;
        }
      }
    }
    
    private void writeToFile(String filepath) throws IOException
    {
         // The name of the file to open.
        FileSystem hdfs = FileSystem.get(conf);
        DataOutputBuffer rest = new DataOutputBuffer();
        Path file = new Path(filepath);
        FSDataOutputStream out;
        
        String starting_tag = "$$$ " + this.filename + ", " + this.current_block ;
        byte[] bytes = new byte[(int) (end - fsin.getPos())];
        fsin.read(fsin.getPos(), bytes, 0, (int) end);

        // if file doesnt exists, then create it
        if (!hdfs.exists(file)) {
            hdfs.createNewFile(file);
        }
      /*  else
        {
            FileWriter fw = new FileWriter(filepath);
            try (BufferedWriter bw = new BufferedWriter(fw)) {
                bw.write(starting_tag + Arrays.toString(bytes));
                bw.close();
                fw.close();
            }
        } */
        out = hdfs.create(file);
        out.write(starting_tag.getBytes());
        out.write(bytes);
        out.close();
        try { 
            DistributedCache.addCacheFile(new URI(filepath), conf);
        } catch (URISyntaxException ex) {
            Logger.getLogger(XmlInputFormatTwoBufferSolution.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
      int i = 0;
      while (true) {
        int b = fsin.read();
        // end of file:
        if (b == -1) {
          return false;
        }
        // save to buffer:
        if (withinBlock) {
          buffer.write(b);
        }

        // check if we're matching:
        if (b == match[i]) {
          i++;
          if (i >= match.length) {
            return true;
          }
        } else {
          i = 0;
        }
        // see if we've passed the stop point:
        if (!withinBlock && i == 0 && fsin.getPos() >= end) {
          return false;
        }
      }
    }
    private int nextBlock() throws IOException
    {
        long pos = fsin.getPos();
        long block_length;
        for (int i=0; i<blocks.length; i++)
        {
            block_length = blocks[i].getOffset() + blocks[i].getLength();
            if (pos == block_length)
            {
                return i+1;
            }
        }
        return 0;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
      return currentKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      return currentValue;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      currentKey = new LongWritable();
      currentValue = new Text();
      return next(currentKey, currentValue);
    }
  }
}
