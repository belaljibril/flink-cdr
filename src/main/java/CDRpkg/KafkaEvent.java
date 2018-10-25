package CDRpkg;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * The event type used in the {@link KafkaRepeatedCalls}.
 *
 * <p>This is a Java POJO, which Flink recognizes and will allow "by-name" field referencing
 * when keying a {@link org.apache.flink.streaming.api.datastream.DataStream} of such a type.
 * For a demonstration of this, see the code in {@link KafkaRepeatedCalls}.
 */
public class KafkaEvent {

    private long timestamp;
    private String a_number;
    private String b_number;
    private String o_cell;
    private String t_cell;
    private int r_flag;
    private long duration;

    public KafkaEvent() {}

    public KafkaEvent(long timestamp, String a_number, String b_number, String o_cell, String t_cell, int r_flag, long duration) {
        this.timestamp = timestamp;
        this.a_number = a_number;
        this.b_number = b_number;
        this.o_cell = o_cell;
        this.t_cell = t_cell;
        this.r_flag = r_flag;
        this.duration = duration;
    }


    public static KafkaEvent fromString(String eventStr) {
        String[] split = eventStr.split(",");
        return new KafkaEvent(
                Long.valueOf(split[0]), split[1], split[2], split[3], split[4], Integer.valueOf(split[5]), Long.valueOf(split[6])
        );
    }

    @Override
    public String toString() {
//        return "timestamp: " + timestamp + ", a_number: " + a_number + ", b_number: " + b_number + ", o_cell: " + o_cell
//                + ", t_cell: " + t_cell + ", r_flag: " + r_flag + ", duration: " + duration;
        return timestamp + "," + a_number + "," + b_number + "," + o_cell + "," + t_cell + "," + r_flag + "," + duration;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getA_number() {
        return a_number;
    }

    public void setA_number(String a_number) {
        this.a_number = a_number;
    }

    public String getB_number() {
        return b_number;
    }

    public void setB_number(String b_number) {
        this.b_number = b_number;
    }

    public String getO_cell() {
        return o_cell;
    }

    public void setO_cell(String o_cell) {
        this.o_cell = o_cell;
    }

    public String getT_cell() {
        return t_cell;
    }

    public void setT_cell(String t_cell) {
        this.t_cell = t_cell;
    }

    public int getR_flag() {
        return r_flag;
    }

    public void setR_flag(int r_flag) {
        this.r_flag = r_flag;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }
}