package CDRpkg;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyProcessWindowFunction extends ProcessWindowFunction<KafkaEvent, KafkaEvent, Tuple, TimeWindow> {

    @Override
    public void process(Tuple key, Context context, Iterable<KafkaEvent> input, Collector<KafkaEvent> out) {
        Long min_ts = Long.MAX_VALUE;
        int count = 0;
        String wo = "";
        for (KafkaEvent in: input) {
            min_ts = Math.min(min_ts, in.getTimestamp());
            count += in.getFrequency();
            wo = in.getWord();
        }
        out.collect(
                new KafkaEvent(wo, count, min_ts)
        );
    }
}