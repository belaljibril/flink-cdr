package CDRpkg;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class FindRepeatedCallsWindowFunction extends ProcessAllWindowFunction<KafkaEvent, KafkaEvent, TimeWindow> {

    @Override
    public void process(Context context, Iterable<KafkaEvent> input, Collector<KafkaEvent> out) {

        ArrayList<KafkaEvent> inputClone = new ArrayList<KafkaEvent>();
        int clone_i = -1;
        for (KafkaEvent inClone: input) {
            clone_i++;
            System.out.println("=======");
            System.out.println("Items[" + clone_i + "]:");
            System.out.println(inClone);
            System.out.println("=======");
            inputClone.add(inClone);
        }

        int el = 0;
        int r_flag = 0;
        ArrayList<KafkaEvent> outEl = new ArrayList<KafkaEvent>();

        int main_input_counter = -1;
        int sub_input_counter = -1;
        ArrayList<Integer> repeated_calls_indicies = new ArrayList<Integer>();
        //System.out.println("My flink FindRepeatedCallsWindowFunction");
        for (KafkaEvent in1: input) {
            main_input_counter++;
            for (KafkaEvent in2: inputClone) {
                sub_input_counter++;
                if(main_input_counter == sub_input_counter) continue;

                if(
                        (in1.getA_number() == in2.getA_number() && in1.getB_number() == in2.getB_number()) ||
                                (in1.getA_number() == in2.getB_number() && in1.getB_number() == in2.getA_number())
                )
                {
                    //System.out.println("My flink found repeated index: " + main_input_counter);
                    if(!repeated_calls_indicies.contains(main_input_counter))
                    {
                        repeated_calls_indicies.add(main_input_counter);
                        System.out.println("=======");
                        System.out.println("Main counter: " + main_input_counter);
                        System.out.println("Sub counter: " + sub_input_counter);
                        System.out.println("My flink found repeated index: " + main_input_counter);
                        System.out.println("Repeated item1: " + in1);
                        System.out.println("Repeated item2: " + in2);
                        System.out.println("=======");
                    }
                }
            }
        }

        int repeated_input_counter = 0;
        for (KafkaEvent in3: input) {
            if(repeated_calls_indicies.contains(repeated_input_counter))
            {
                KafkaEvent o = new KafkaEvent(
                        in3.getTimestamp(),
                        in3.getA_number(),
                        in3.getB_number(),
                        in3.getO_cell(),
                        in3.getT_cell(),
                        1,
                        in3.getDuration(),
                        in3.getO_long(),
                        in3.getO_lat(),
                        in3.getO_emi(),
                        in3.getT_long(),
                        in3.getT_lat(),
                        in3.getT_emi()
                );
                out.collect(o);
            }
            repeated_input_counter++;
        }
    }
}