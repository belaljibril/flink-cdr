package CDRpkg;

import org.apache.flink.api.common.functions.AggregateFunction;

public class AverageAggregate /*implements AggregateFunction<KafkaEvent, KafkaEvent, KafkaEvent>*/ {
    /*
    @Override
    public KafkaEvent createAccumulator() {
        return new KafkaEvent();
    }

    @Override
    public KafkaEvent add(KafkaEvent value, KafkaEvent accumulator) {
        return new KafkaEvent(value.getWord(), accumulator.getFrequency() + value.getFrequency(), Math.min(accumulator.getTimestamp(), value.getTimestamp()));
    }

    @Override
    public KafkaEvent getResult(KafkaEvent accumulator) {
        return accumulator;
    }

    @Override
    public KafkaEvent merge(KafkaEvent a, KafkaEvent b) {
        return new KafkaEvent(a.getWord(), a.getFrequency() + b.getFrequency(), (a.getTimestamp() + b.getTimestamp())/2 ) ;
    }*/
}
