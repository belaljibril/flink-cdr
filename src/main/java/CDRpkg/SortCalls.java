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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;

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



public class SortCalls {


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
//		env.enableCheckpointing(60000); // create a checkpoint every 5 seconds
		env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
		env.setStateBackend(new MemoryStateBackend(MAX_MEM_STATE_SIZE));

        DataStream<KafkaEventIn> input = env
				.addSource(
						new FlinkKafkaConsumer010<>(
								parameterTool.getRequired("input-topic"),
								new KafkaEventSchemaIn(),
								parameterTool.getProperties())
                .setStartFromEarliest()
                .assignTimestampsAndWatermarks(new CustomWatermarkExtractor()))
                .keyBy("dummyendfield")
				.process(new SortFunction())
				;



        input.addSink(
				new FlinkKafkaProducer010<>(
						parameterTool.getRequired("output-topic"),
						new KafkaEventSchemaIn(),
						parameterTool.getProperties()));

		env.execute("Sort-Calls");
	}


	/**
	 * A custom {@link AssignerWithPeriodicWatermarks}, that simply assumes that the input stream
	 * records are strictly ascending.
	 *
	 * <p>Flink also ships some built-in convenience assigners, such as the
	 * {@link BoundedOutOfOrdernessTimestampExtractor} and {@link AscendingTimestampExtractor}
	 */
	private static class CustomWatermarkExtractor implements AssignerWithPeriodicWatermarks<KafkaEventIn> {

		private static final long serialVersionUID = -742759155861320823L;

        private final long maxOutOfOrderness = 900000; // 15 * 60 seconds

        private long currentMaxTimestamp;


//		@Override
//		public long extractTimestamp(KafkaEventIn event, long previousElementTimestamp) {
//			return event.getUnixtimestamp();
//		}

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

//		@Nullable
//		@Override
//		public Watermark checkAndGetNextWatermark(KafkaEventIn lastElement, long extractedTimestamp) {
//			return new Watermark(extractedTimestamp - 30000);
//		}
	}

	public static class SortFunction extends KeyedProcessFunction<Tuple, KafkaEventIn, KafkaEventIn> {
		private ValueState<PriorityQueue<KafkaEventIn>> queueState = null;

		@Override
		public void open(Configuration config) {
			ValueStateDescriptor<PriorityQueue<KafkaEventIn>> descriptor = new ValueStateDescriptor<>(
					// state name
					"sorted-events",
					// type information of state
					TypeInformation.of(new TypeHint<PriorityQueue<KafkaEventIn>>() {
					}));
			queueState = getRuntimeContext().getState(descriptor);
		}

		@Override
		public void processElement(KafkaEventIn event, Context context, Collector<KafkaEventIn> out) throws Exception {
			TimerService timerService = context.timerService();

			if (context.timestamp() > timerService.currentWatermark()) {
				PriorityQueue<KafkaEventIn> queue = queueState.value();
				if (queue == null) {
					queue = new PriorityQueue<>(1000000);
				}
				queue.add(event);
				queueState.update(queue);
				timerService.registerEventTimeTimer(event.getUnixtimestamp());
			}
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext context, Collector<KafkaEventIn> out) throws Exception {
			PriorityQueue<KafkaEventIn> queue = queueState.value();
			Long watermark = context.timerService().currentWatermark();
			KafkaEventIn head = queue.peek();
            while (head != null && head.getUnixtimestamp() <= watermark) {
				out.collect(head);
				queue.remove(head);
				head = queue.peek();
			}
		}
	}

	public static class SortAllFunction extends ProcessFunction<KafkaEventIn, KafkaEventIn> {
		private ValueState<PriorityQueue<KafkaEventIn>> queueState = null;

		@Override
		public void open(Configuration config) {
			ValueStateDescriptor<PriorityQueue<KafkaEventIn>> descriptor = new ValueStateDescriptor<>(
					// state name
					"sorted-events",
					// type information of state
					TypeInformation.of(new TypeHint<PriorityQueue<KafkaEventIn>>() {
					}));
			queueState = getRuntimeContext().getState(descriptor);
		}

		@Override
		public void processElement(KafkaEventIn event, Context context, Collector<KafkaEventIn> out) throws Exception {
			TimerService timerService = context.timerService();

			if (context.timestamp() > timerService.currentWatermark()) {
				PriorityQueue<KafkaEventIn> queue = queueState.value();
				if (queue == null) {
					queue = new PriorityQueue<>(1000);
				}
				queue.add(event);
				queueState.update(queue);
				timerService.registerEventTimeTimer(event.getUnixtimestamp());
			}
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext context, Collector<KafkaEventIn> out) throws Exception {
			PriorityQueue<KafkaEventIn> queue = queueState.value();
			Long watermark = context.timerService().currentWatermark();
			KafkaEventIn head = queue.peek();
			while (head != null && head.getUnixtimestamp() <= watermark) {
				out.collect(head);
				queue.remove(head);
				head = queue.peek();
			}
		}
	}

}
