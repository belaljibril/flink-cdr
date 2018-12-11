/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package CDRpkg;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */



public class RepeatedCalls {


	public static void main(String[] args) throws Exception {
		// parse input arguments
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		if (parameterTool.getNumberOfParameters() < 5) {
			System.out.println("Missing parameters!\n" +
					"Usage: Kafka --input-topic <topic> --output-topic <topic> " +
					"--bootstrap.servers <kafka brokers> " +
					"--zookeeper.connect <zk quorum> --group.id <some id>");
			return;
		}

		int MAX_MEM_STATE_SIZE = Integer.MAX_VALUE; //_MAX 4 * 1024 * 1024 * 1024;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		env.enableCheckpointing(60000); // create a checkpoint every 5 seconds
		env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
//		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//        env.setParallelism(1);
		env.setStateBackend(new MemoryStateBackend(MAX_MEM_STATE_SIZE));

        DataStream<KafkaEventOut> input = env
				.addSource(
						new FlinkKafkaConsumer010<>(
								parameterTool.getRequired("input-topic"),
								new KafkaEventSchemaIn(),
								parameterTool.getProperties())
                .setStartFromEarliest()
//				.assignTimestampsAndWatermarks(new CustomWatermarkExtractor())
                )

                .keyBy("anumber", "bnumber")
                .countWindow(1,1)
                .process(new ProcessWindowFunction<KafkaEventIn, KafkaEventOut, Tuple, GlobalWindow>() {


                    private transient ValueState<KafkaEventIn> historyElement;

                    @Override
                    public void open(Configuration config) {

                        StateTtlConfig ttlConfig = StateTtlConfig
                                .newBuilder(org.apache.flink.api.common.time.Time.seconds(30))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .cleanupFullSnapshot()
                                .build();


                        ValueStateDescriptor<KafkaEventIn> valueDescriptor =
                                new ValueStateDescriptor<>(
                                        "historyElement", // the state name
                                        TypeInformation.of(new TypeHint<KafkaEventIn>() {})
                                );

                        valueDescriptor.enableTimeToLive(ttlConfig);

                        historyElement = getRuntimeContext().getState(valueDescriptor);
                    }

                    @Override
                    public void process(Tuple tuple, Context context, Iterable<KafkaEventIn> input, Collector<KafkaEventOut> out) throws Exception {
                        KafkaEventIn iWindow = null;
                        KafkaEventIn iHistory = null;
						KafkaEventOut iOutput = null;

                        Iterator<KafkaEventIn> it = input.iterator();

                        if(it.hasNext()) {
                            iWindow = KafkaEventIn.fromString(it.next().toString());
                            iOutput = new KafkaEventOut(iWindow);
                        }

//                        if(!iWindow.filter()) return;

                        if(historyElement.value() != null)
                        {
                            iHistory = historyElement.value();
                        }


                        boolean is_repeated = KafkaEventIn.areCallsRepeated(iWindow, iHistory);
                        if(is_repeated)
                        {
							iOutput.setRflag(1);
                        }
                        out.collect(iOutput);

                        historyElement.update(iWindow);
                    }
                });

        input.addSink(
				new FlinkKafkaProducer010<>(
						parameterTool.getRequired("output-topic"),
						new KafkaEventSchemaOut(),
						parameterTool.getProperties()));

		env.execute("Repeated-Calls");
	}

	/**
	 * A {@link RichMapFunction} that continuously outputs the current total frequency count of a key.
	 * The current total count is keyed state managed by Flink.
	 */
	/*private static class RollingAdditionMapper extends RichMapFunction<KafkaEventIn, KafkaEventIn> {

		private static final long serialVersionUID = 1180234853172462378L;

		private transient ValueState<Integer> currentTotalCount;

		@Override
		public KafkaEventIn map(KafkaEventIn event) throws Exception {
			Integer totalCount = currentTotalCount.value();

			if (totalCount == null) {
				totalCount = 0;
			}
			totalCount += event.getFrequency();

			currentTotalCount.update(totalCount);

			return new KafkaEventIn(event.getWord(), totalCount, event.getTimestamp());
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			currentTotalCount = getRuntimeContext().getState(new ValueStateDescriptor<>("currentTotalCount", Integer.class));
		}
	}*/

	/**
	 * A custom {@link AssignerWithPeriodicWatermarks}, that simply assumes that the input stream
	 * records are strictly ascending.
	 *
	 * <p>Flink also ships some built-in convenience assigners, such as the
	 * {@link BoundedOutOfOrdernessTimestampExtractor} and {@link AscendingTimestampExtractor}
	 */
	private static class CustomWatermarkExtractorOriginal implements AssignerWithPunctuatedWatermarks<KafkaEventIn> {

		private static final long serialVersionUID = -742759155861320820L;

		@Override
		public long extractTimestamp(KafkaEventIn event, long previousElementTimestamp) {
			return event.getUnixtimestamp();
		}

		/*
		@Nullable
		@Override
		public Watermark getCurrentWatermark() {
			return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
		}
		*/

		@Nullable
		@Override
		public Watermark checkAndGetNextWatermark(KafkaEventIn lastElement, long extractedTimestamp) {
			return new Watermark(extractedTimestamp - 300000);
		}
	}

	private static class CustomWatermarkExtractor implements AssignerWithPeriodicWatermarks<KafkaEventIn> {

		private static final long serialVersionUID = -742759155861320822L;

		private final long maxOutOfOrderness = 5000; // 5 seconds

		private long currentMaxTimestamp;


		@Override
		public long extractTimestamp(KafkaEventIn element, long previousElementTimestamp) {
			long timestamp = element.getUnixtimestamp();
			currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
			return timestamp;
		}

		@Override
		public Watermark getCurrentWatermark() {
			// return the watermark as current highest timestamp minus the out-of-orderness bound
			return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
		}

	}

}
