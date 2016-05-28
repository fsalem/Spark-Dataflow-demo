package spark.dataflow;

import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class WordCountHadoopTransform
		extends
		PTransform<PCollection<KV<String, String>>, PCollection<KV<Text, LongWritable>>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1874122535644822187L;

	@Override
	public PCollection<KV<Text, LongWritable>> apply(
			PCollection<KV<String, String>> lines) {
		PCollection<String> words = lines.apply(ParDo.of(new ExtractWordFn()));
		PCollection<KV<String, Long>> wordCounts = words.apply(Count
				.<String> perElement());
		return wordCounts.apply(ParDo
				.of(new DoFn<KV<String, Long>, KV<Text, LongWritable>>() {

					@Override
					public void processElement(
							DoFn<KV<String, Long>, KV<Text, LongWritable>>.ProcessContext c)
							throws Exception {
						c.output(KV.of(new Text(c.element().getKey()),
								new LongWritable(c.element().getValue())));
					}
				}));
	}
}
