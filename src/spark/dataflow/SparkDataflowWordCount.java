package spark.dataflow;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import kafka.serializer.StringDecoder;

import org.apache.beam.runners.spark.EvaluationResult;
import org.apache.beam.runners.spark.SparkPipelineRunner;
import org.apache.beam.runners.spark.SparkStreamingPipelineOptions;
import org.apache.beam.runners.spark.io.KafkaIO;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import utils.PropertiesStack;

public class SparkDataflowWordCount {

	public static void main(String[] args) {

		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list",
				PropertiesStack.getKafkaBootstrapServers());
		kafkaParams.put("zookeeper.connect",
				PropertiesStack.getZookeeperConnect());
		kafkaParams.put("auto.offset.reset", "smallest");
		kafkaParams.put("group.id", PropertiesStack.getKafkaGroupId());
		kafkaParams.put("auto.commit.enable", "false");

		SparkStreamingPipelineOptions options = PipelineOptionsFactory
				.fromArgs(args).withValidation()
				.as(SparkStreamingPipelineOptions.class);
		options.setRunner(SparkPipelineRunner.class);
		options.setStreaming(Boolean.TRUE);
		Pipeline p = Pipeline.create(options);
		p.begin();
		PCollection<KV<String, String>> records = p.apply(
				KafkaIO.Read.from(StringDecoder.class, StringDecoder.class,
						String.class, String.class, Collections
								.singleton(PropertiesStack
										.getKafkaTopic()), kafkaParams))
				.setCoder(
						KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

		PCollection<KV<String, String>> windowInput = records
		// .apply(Window.<String>
		// into(FixedWindows.of(Duration.standardMinutes(1))));
				.apply(Window
						.<KV<String, String>> into(
								FixedWindows.of(Duration.standardSeconds(10)))
						.triggering(AfterWatermark.pastEndOfWindow()
						// .plusDelayOf(Duration.standardMinutes(1))
						).discardingFiredPanes()
						.withAllowedLateness(Duration.ZERO));

		PCollection<String> wordCounts = windowInput
				.apply(new WordCountTextTransform());

		wordCounts.apply(ParDo.of(new DoFn<String, String>() {

			@Override
			public void processElement(DoFn<String, String>.ProcessContext c)
					throws Exception {
				System.out.println(c.element());
			}

		}));

		// PCollection<KV<Text, LongWritable>> wordCounts = windowInput.apply(
		// new WordCountTransform()).setCoder(
		// KvCoder.of(WritableCoder.of(Text.class),
		// WritableCoder.of(LongWritable.class)));

		// Class<? extends FileOutputFormat<Text, LongWritable>>
		// outputFormatClass = (Class<? extends FileOutputFormat<Text,
		// LongWritable>>) (Class<?>) TemplatedSequenceFileOutputFormat.class;
		//
		// HadoopIO.Write.Bound<Text, LongWritable> write = HadoopIO.Write.to(
		// "/spark/dataflow/", outputFormatClass,
		// Text.class, LongWritable.class);
		// wordCounts.apply(write.withoutSharding());
		EvaluationResult res = SparkPipelineRunner.create(options).run(p);
		res.close();

	}
}