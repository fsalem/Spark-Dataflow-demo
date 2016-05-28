package spark.dataflow;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class CombineWordCountFn extends DoFn<KV<String, Long>, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 311222424158594211L;

	@Override
	public void processElement(DoFn<KV<String, Long>, String>.ProcessContext c) throws Exception {
		c.output(c.element().getKey()+"\t"+c.element().getValue());
	}

}
