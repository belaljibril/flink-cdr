package CDRpkg;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

public class MyProcessWindowFunction extends ProcessWindowFunction<KafkaEvent, KafkaEvent, Tuple, TimeWindow> {

    @Override
    public void process(Tuple key, Context context, Iterable<KafkaEvent> input, Collector<KafkaEvent> out) {

        int el = 0;
        int r_flag = 0;
        ArrayList<KafkaEvent> outEl = new ArrayList<KafkaEvent>();
        for (KafkaEvent in: input) {
            KafkaEvent o = new KafkaEvent(
                    in.getTimestamp(),
                    in.getA_number(),
                    in.getB_number(),
                    in.getO_cell(),
                    in.getT_cell(),
                    in.getR_flag(),
                    in.getDuration()
            );
            outEl.add(o);
            el ++;
        }
        r_flag = (el > 1) ? 1 : 0;

        for(KafkaEvent o2: outEl)
        {
            out.collect(
                new KafkaEvent(
                        o2.getTimestamp(),
                        o2.getA_number(),
                        o2.getB_number(),
                        o2.getO_cell(),
                        o2.getT_cell(),
                        r_flag,
                        o2.getDuration()
                )
            );

        }

    }
}