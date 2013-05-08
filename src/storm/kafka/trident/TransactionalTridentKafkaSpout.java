package storm.kafka.trident;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.DynamicPartitionConnections;
import storm.kafka.GlobalPartitionId;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.Config;
import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;


public class TransactionalTridentKafkaSpout implements IPartitionedTridentSpout<Map<String, List>, GlobalPartitionId, Map> {
    
    TridentKafkaConfig _config;
    String _topologyInstanceId = UUID.randomUUID().toString();
    public static final Logger LOG = LoggerFactory.getLogger(TransactionalTridentKafkaSpout.class);

    public TransactionalTridentKafkaSpout(TridentKafkaConfig config) {
        _config = config;
    }
    
    class Coordinator implements IPartitionedTridentSpout.Coordinator<Map> {
        IBrokerReader reader;
        
        public Coordinator(Map conf) {
            reader = KafkaUtils.makeBrokerReader(conf, _config);
        }
        
        public void close() {
            _config.coordinator.close();
        }

        public boolean isReady(long txid) {
            return _config.coordinator.isReady(txid);
        }

        public Map getPartitionsForBatch() {
           return reader.getCurrentBrokers();
        }
    }
    
    class Emitter implements IPartitionedTridentSpout.Emitter<Map<String, List>, GlobalPartitionId, Map> {
        DynamicPartitionConnections _connections;
        String _topologyName;
        TopologyContext _context;
        KafkaUtils.KafkaOffsetMetric _kafkaOffsetMetric;
        ReducedMetric _kafkaMeanFetchLatencyMetric;
        CombinedMetric _kafkaMaxFetchLatencyMetric;

        public Emitter(Map conf, TopologyContext context) {
            _connections = new DynamicPartitionConnections(_config);
            _topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
            _context = context;
            _kafkaOffsetMetric = new KafkaUtils.KafkaOffsetMetric(_config.topic, _connections);
            context.registerMetric("kafkaOffset", _kafkaOffsetMetric, 60);
            _kafkaMeanFetchLatencyMetric = context.registerMetric("kafkaFetchAvg", new MeanReducer(), 60);
            _kafkaMaxFetchLatencyMetric = context.registerMetric("kafkaFetchMax", new MaxMetric(), 60);
        }
        
        public Map emitPartitionBatchNew(TransactionAttempt attempt, TridentCollector collector, GlobalPartitionId partition, Map lastMeta) {
            SimpleConsumer consumer = _connections.register(partition);
            Map ret = KafkaUtils.emitPartitionBatchNew(_config, consumer, partition, collector, lastMeta, _topologyInstanceId, _topologyName, _kafkaMeanFetchLatencyMetric, _kafkaMaxFetchLatencyMetric);
            _kafkaOffsetMetric.setLatestEmittedOffset(partition, (Long)ret.get("offset"));
            return ret;
        }

        public void emitPartitionBatch(TransactionAttempt attempt, TridentCollector collector, GlobalPartitionId partition, Map meta) {
            String instanceId = (String) meta.get("instanceId");
            if(!_config.forceFromStart || instanceId.equals(_topologyInstanceId)) {
                SimpleConsumer consumer = _connections.register(partition);
                long offset = (Long) meta.get("offset");
                long nextOffset = (Long) meta.get("nextOffset");
                long start = System.nanoTime();
                ByteBufferMessageSet msgs = consumer.fetch(new FetchRequest(_config.topic, partition.partition, offset, _config.fetchSizeBytes));
                long end = System.nanoTime();
                long millis = (end - start) / 1000000;
                _kafkaMeanFetchLatencyMetric.update(millis);
                _kafkaMaxFetchLatencyMetric.update(millis);

                for(MessageAndOffset msg: msgs) {
                    if(offset == nextOffset) break;
                    if(offset > nextOffset) {
                        throw new RuntimeException("Error when re-emitting batch. overshot the end offset");
                    }
                    KafkaUtils.emit(_config, collector, msg.message());
                    offset = msg.offset();
                }        
            }
        }
        
        public void close() {
            _connections.clear();
        }

        public List<GlobalPartitionId> getOrderedPartitions(Map<String, List> partitions) {
            return KafkaUtils.getOrderedPartitions(partitions);
        }

        public void refreshPartitions(List<GlobalPartitionId> list) {
            _connections.clear();
            _kafkaOffsetMetric.refreshPartitions(new HashSet<GlobalPartitionId>(list));
        }
    }
    

    public IPartitionedTridentSpout.Coordinator getCoordinator(Map conf, TopologyContext context) {
        return new Coordinator(conf);
    }

    public IPartitionedTridentSpout.Emitter getEmitter(Map conf, TopologyContext context) {
        return new Emitter(conf, context);
    }

    public Fields getOutputFields() {
        return _config.scheme.getOutputFields();
    }
        
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}