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

//        System.out.println("==========================");
//        System.out.println("Window calls");
        for (int main_input_counter = 0; main_input_counter < castedInput.size(); main_input_counter++) {
            KafkaEvent in1 = castedInput.get(main_input_counter);
//            System.out.println("Compared call[" + main_input_counter + "]: " + in1);
            for (int sub_input_counter = main_input_counter+1; sub_input_counter < castedInput.size(); sub_input_counter++) {
                KafkaEvent in2 = castedInput.get(sub_input_counter);
//                System.out.println("==========================");
//                System.out.println("Compared call1: " + in1);
//                System.out.println("Compared call2: " + in2);
//                System.out.println("==========================");
                if(
                        (in1.getAnumber().equals(in2.getAnumber()) && in1.getBnumber().equals(in2.getBnumber())) ||
                                (in1.getAnumber().equals(in2.getBnumber()) && in1.getBnumber().equals(in2.getAnumber()))
                )
                {
                    in1.setRflag(1);
                    in2.setRflag(1);

                    out.collect(in1);
                    out.collect(in2);

//                    System.out.println("==========================");
//                    System.out.println("Repeated call1: " + in1);
//                    System.out.println("Repeated call2: " + in2);
//                    System.out.println("==========================");
                }
            }
        }
//        System.out.println("==========================");

    }
}