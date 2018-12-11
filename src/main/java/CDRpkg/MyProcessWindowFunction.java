package CDRpkg;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyProcessWindowFunction extends ProcessWindowFunction<KafkaEventIn, KafkaEventIn, Tuple, TimeWindow> {

    @Override
    public void process(Tuple key, Context context, Iterable<KafkaEventIn> input, Collector<KafkaEventIn> out) {
/*
        ArrayList<KafkaEventIn> castedInput = Lists.newArrayList(input);
        int el_count = castedInput.size();
        int r_flag = (el_count > 1) ? 1 : 0;
        if(r_flag == 0)
        {
            return;
        }

        for(KafkaEventIn o2: castedInput)
        {
            out.collect(
                new KafkaEventIn(
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