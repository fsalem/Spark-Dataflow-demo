package spark.dataflow;

import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class WordCountTextTransform extends
		PTransform<PCollection<KV<String, String>>, PCollection<String>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1874122535644822187L;

	@Override
	public PCollection<String> apply(PCollection<KV<String, String>> lines) {
		PCollection<String> words = lines.apply(ParDo.of(new ExtractWordFn()));
		PCollection<KV<String, Long>> wordCounts = words.apply(Count
				.<String> perElement());
		return wordCounts.apply(ParDo.of(new DoFn<KV<String, Long>, String>() {

			@Override
			public void processElement(
					DoFn<KV<String, Long>, String>.ProcessContext c)
					throws Exception {
				c.output(c.element().getKey() + "\t" + c.element().getValue());
			}
		}));
	}
}
