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

import avro.shaded.com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.ReduceFunction;
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
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

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



public class KafkaDetectRepeatedCallsUnique {


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

		int MAX_MEM_STATE_SIZE = 1 * 1024 * 1024 * 1024;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
		env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
//		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setParallelism(1);
		env.setStateBackend(new MemoryStateBackend(MAX_MEM_STATE_SIZE));

        DataStream<KafkaEvent> input = env
				.addSource(
						new FlinkKafkaConsumer010<>(
								parameterTool.getRequired("input-topic"),
								new KafkaEventSchema(),
								parameterTool.getProperties())
								.setStartFromEarliest()
								.assignTimestampsAndWatermarks(new CustomWatermarkExtractor()))
                .keyBy("anumber", "bnumber")
                .countWindow(1,1)
//                .window(GlobalWindows.create())
//                .trigger(CountTrigger.of(2))
//                .evictor(CountEvictor.of(2))
                .process(new ProcessWindowFunction<KafkaEvent, KafkaEvent, Tuple, GlobalWindow>() {

                    private transient ListState<KafkaEvent> elements;

                    private transient ValueState<KafkaEvent> historyElement;

                    @Override
                    public void open(Configuration config) {

                        StateTtlConfig ttlConfig = StateTtlConfig
                                .newBuilder(org.apache.flink.api.common.time.Time.seconds(5))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .cleanupFullSnapshot()
                                .build();


                        ListStateDescriptor<KafkaEvent> listDescriptor =
                                new ListStateDescriptor<>(
                                        "collectedElements", // the state name
                                        TypeInformation.of(new TypeHint<KafkaEvent>() {})

                                );

                        ValueStateDescriptor<KafkaEvent> valueDescriptor =
                                new ValueStateDescriptor<>(
                                        "historyElement", // the state name
                                        TypeInformation.of(new TypeHint<KafkaEvent>() {})
                                );

                        listDescriptor.enableTimeToLive(ttlConfig);
                        valueDescriptor.enableTimeToLive(ttlConfig);

                        elements = getRuntimeContext().getListState(listDescriptor);
                        historyElement = getRuntimeContext().getState(valueDescriptor);
                    }

                    @Override
                    public void process(Tuple tuple, Context context, Iterable<KafkaEvent> input, Collector<KafkaEvent> out) throws Exception {
                        KafkaEvent iWindow = null;
                        KafkaEvent iHistory = null;

                        Iterator<KafkaEvent> it = input.iterator();

                        if(it.hasNext()) {
                            iWindow = KafkaEvent.fromString(it.next().toString());
                        }

                        if(historyElement.value() != null)
                        {
                            iHistory = historyElement.value();
                        }

//                        ArrayList<KafkaEvent> AllEls = Lists.newArrayList(elements.get());


//                        if(it.hasNext()) {
//                            i2 = KafkaEvent.fromString(it.next().toString());
//                        }

                        boolean is_repeated = KafkaEvent.areCallsRepeated(iWindow, iHistory);
                        if(is_repeated)
                        {
                            iWindow.setRflag(1);
//                            iHistory.setRflag(1);
                        }

                        out.collect(iWindow);
                        historyElement.update(iWindow);


                        /*if(i1 != null && !i1.isNull())
                        {
                            elements.add(i1);
                        }*/
//                        if(i2 != null && !i2.isNull() && !AllEls.contains(i2))
//                        {
//                            out.collect(i2);
//                        }
                        // TODO: check if previously i2 was repeated
                        // Then set rflag = 1 from history
                        /*if(i2 != null && !i2.isNull() && !AllEls.contains(i2))
                        {
                            out.collect(i2);
                        }*/

                    }
                })
                ;


        input.addSink(
				new FlinkKafkaProducer010<>(
						parameterTool.getRequired("output-topic"),
						new KafkaEventSchema(),
						parameterTool.getProperties()));

		env.execute("Kafka 0.10 Example");
	}

	/**
	 * A {@link RichMapFunction} that continuously outputs the current total frequency count of a key.
	 * The current total count is keyed state managed by Flink.
	 */
	/*private static class RollingAdditionMapper extends RichMapFunction<KafkaEvent, KafkaEvent> {

		private static final long serialVersionUID = 1180234853172462378L;

		private transient ValueState<Integer> currentTotalCount;

		@Override
		public KafkaEvent map(KafkaEvent event) throws Exception {
			Integer totalCount = currentTotalCount.value();

			if (totalCount == null) {
				totalCount = 0;
			}
			totalCount += event.getFrequency();

			currentTotalCount.update(totalCount);

			return new KafkaEvent(event.getWord(), totalCount, event.getTimestamp());
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
	private static class CustomWatermarkExtractor implements AssignerWithPeriodicWatermarks<KafkaEvent> {

		private static final long serialVersionUID = -742759155861320823L;

		private long currentTimestamp = Long.MIN_VALUE;

		@Override
		public long extractTimestamp(KafkaEvent event, long previousElementTimestamp) {
			// the inputs are assumed to be of format (message,timestamp)

            String ts = event.getEstimestamp();
            DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ENGLISH);
            Date numeric_ts = new Date(Long.MIN_VALUE);
            try {
                numeric_ts = format.parse(ts);
            } catch (ParseException e) {}

            this.currentTimestamp = numeric_ts.getTime();
			return numeric_ts.getTime();
		}

		@Nullable
		@Override
		public Watermark getCurrentWatermark() {
			return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
		}
	}


}
