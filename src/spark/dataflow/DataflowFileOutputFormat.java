package spark.dataflow;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

public class DataflowFileOutputFormat<K,V> extends FileOutputFormat<K, V> {

	@Override
	public RecordWriter<K, V> getRecordWriter(FileSystem arg0, JobConf arg1,
			String arg2, Progressable arg3) throws IOException {
		return null;
	}

}
