package CDRpkg;

import avro.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

public class FindRepeatedCallsWindowFunction extends ProcessAllWindowFunction<KafkaEvent, KafkaEvent, TimeWindow> {

    @Override
    public void process(Context context, Iterable<KafkaEvent> input, Collector<KafkaEvent> out) {

        ArrayList<Integer> repeated_calls_indicies = new ArrayList<Integer>();

        ArrayList<KafkaEvent> castedInput = Lists.newArrayList(input);

        for (int main_input_counter = 0; main_input_counter < castedInput.size(); main_input_counter++) {
            KafkaEvent in1 = castedInput.get(main_input_counter);
            for (int sub_input_counter = main_input_counter+1; sub_input_counter < castedInput.size(); sub_input_counter++) {
                KafkaEvent in2 = castedInput.get(sub_input_counter);
                if(
                        (in1.getA_number().equals(in2.getA_number()) && in1.getB_number().equals(in2.getB_number())) ||
                                (in1.getA_number().equals(in2.getB_number()) && in1.getB_number().equals(in2.getA_number()))
                )
                {
                    if(!repeated_calls_indicies.contains(main_input_counter))
                    {
                        repeated_calls_indicies.add(main_input_counter);
                    }
                    if(!repeated_calls_indicies.contains(sub_input_counter))
                    {
                        repeated_calls_indicies.add(sub_input_counter);
                    }
                }
            }
        }

        Collections.sort(repeated_calls_indicies);

        for (int repeated_input_counter = 0; repeated_input_counter < castedInput.size(); repeated_input_counter++) {
            KafkaEvent in3 = castedInput.get(repeated_input_counter);
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
        }
    }
}