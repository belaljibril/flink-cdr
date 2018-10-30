package CDRpkg;

import avro.shaded.com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

public class MyProcessWindowFunction extends ProcessWindowFunction<KafkaEvent, KafkaEvent, Tuple, TimeWindow> {

    @Override
    public void process(Tuple key, Context context, Iterable<KafkaEvent> input, Collector<KafkaEvent> out) {
/*
        ArrayList<KafkaEvent> castedInput = Lists.newArrayList(input);
        int el_count = castedInput.size();
        int r_flag = (el_count > 1) ? 1 : 0;
        if(r_flag == 0)
        {
            return;
        }

        for(KafkaEvent o2: castedInput)
        {
            out.collect(
                new KafkaEvent(
                        o2.getTimestamp(),
                        o2.getA_number(),
                        o2.getB_number(),
                        o2.getO_cell(),
                        o2.getT_cell(),
                        r_flag,
                        o2.getDuration(),
                        o2.getO_long(),
                        o2.getO_lat(),
                        o2.getO_emi(),
                        o2.getT_long(),
                        o2.getT_lat(),
                        o2.getT_emi()
                )
            );

        }
*/
    }
}