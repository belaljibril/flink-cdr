package CDRpkg;

import avro.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

public class FindRepeatedCallsWindowFunction extends ProcessAllWindowFunction<KafkaEvent, KafkaEvent, TimeWindow> {

    @Override
    public void process(Context context, Iterable<KafkaEvent> input, Collector<KafkaEvent> out) {

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
                    in1.setR_flag(1);
                    in2.setR_flag(1);

                    out.collect(in1);
                    out.collect(in2);
                }
            }
        }
    }
}