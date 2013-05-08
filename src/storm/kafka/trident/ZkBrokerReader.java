package storm.kafka.trident;

import java.util.List;
import java.util.Map;

import storm.kafka.DynamicBrokersReader;
import storm.kafka.KafkaConfig.ZkHosts;


public class ZkBrokerReader implements IBrokerReader {

    Map<String, List<Long>> cachedBrokers;
    DynamicBrokersReader reader;
    long lastRefreshTimeMs;
    long refreshMillis;
    
    public ZkBrokerReader(Map<String,Object> conf, String topic, ZkHosts hosts) {
        reader = new DynamicBrokersReader(conf, hosts.brokerZkStr, hosts.brokerZkPath, topic);
        cachedBrokers = reader.getBrokerInfo();
        lastRefreshTimeMs = System.currentTimeMillis();
        refreshMillis = hosts.refreshFreqSecs * 1000L;
    }
    
    public Map<String, List<Long>> getCurrentBrokers() {
        long currTime = System.currentTimeMillis();
        if(currTime > lastRefreshTimeMs + refreshMillis) {
            cachedBrokers = reader.getBrokerInfo();
            lastRefreshTimeMs = currTime;
        }
        return cachedBrokers;
    }

    public void close() {
        reader.close();
    }    
}
