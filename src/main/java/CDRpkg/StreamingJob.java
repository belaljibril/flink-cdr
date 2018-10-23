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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
//import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.CsvAppendTableSourceFactory;
import java.util.Properties;



import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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



public class StreamingJob {





	public static void main(String[] args) throws Exception {



		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


			//Add Kafka connection cli arguments in menu: Run->Edit Configurations->Program arguments
		ParameterTool myArgs = ParameterTool.fromArgs(args);



		//Define Flink Kafka Consumer
		FlinkKafkaConsumer011<String> myConsumer = 	new FlinkKafkaConsumer011<>(
				"tt1",
				new SimpleStringSchema(),

				myArgs.getProperties()			//arguments added here
		);





		myConsumer.setStartFromEarliest();     // start from the earliest record possible





			//Define Flink Kafka Producer
		FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>(
				"localhost:9092",            // broker list
				"tt2",                  // target topic
				new SimpleStringSchema());   // serialization schema




		//Create DataStream object using "DataStream" API, and add to it a Kafka Consumer by using env.AddSource method
		DataStreamSink<String> myStream = env

				.addSource(myConsumer)

				//.print();



				 .map(
				new MapFunction<String, String>() {
					private static final long serialVersionUID = -6867736771747690202L;
					@Override
					public String map(String value) throws Exception {
						return "Kafka and Flink says: " + value;
					}
				})


				.filter(new FilterFunction<String>() {
					private static final long serialVersionUID = -6867736771747690202L;
					@Override
					public boolean filter(String value) throws Exception {
						return value.contains("hi");
					}
				})

			.addSink(myProducer);






		//Add the producer as Sink to the same stream
		//myStream.addSink(myProducer);




		//myStream.print()

		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
