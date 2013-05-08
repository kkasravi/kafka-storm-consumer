package storm.kafka.trident;


import backtype.storm.metric.api.ICombiner;

public class MaxMetric implements ICombiner<Long> {
    public Long identity() {
        return null;
    }

    public Long combine(Long l1, Long l2) {
        if(l1 == null) return l2;
        if(l2 == null) return l1;
        return Math.max(l1, l2);
    }

}
