package storm.kafka.trident;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.kafka.HostPort;
import storm.kafka.KafkaConfig.StaticHosts;


public class StaticBrokerReader implements IBrokerReader {

    Map<String, List<Long>> brokers = new HashMap<String,List<Long>>();
    
    public StaticBrokerReader(StaticHosts hosts) {
        for(HostPort hp: hosts.hosts) {
            List<Long> info = new ArrayList<Long>();
            info.add((long) hp.port);
            info.add((long) hosts.partitionsPerHost);
            brokers.put(hp.host, info);
        }
    }
    
    public Map<String, List<Long>> getCurrentBrokers() {
        return brokers;
    }

    public void close() {
    }
}
