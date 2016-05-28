package spark.dataflow;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class ExtractWordFn extends DoFn<KV<String, String>, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 311222424158594211L;

	@Override
	public void processElement(DoFn<KV<String, String>, String>.ProcessContext c) throws Exception {
		for (String word : c.element().getValue().split("[^a-zA-Z']+")) {
			if (!word.isEmpty()) {
				c.output(word);
			}
		}

	}

}
