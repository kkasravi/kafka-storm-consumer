package storm.kafka;

import java.util.List;

import storm.kafka.PartitionManager.KafkaMessageId;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

public class ConsumerStream implements Runnable {

    private KafkaStream<byte[],byte[]> m_stream;
    private int m_threadNumber;
    SpoutOutputCollector collector;
    SpoutConfig _spoutConfig;
    String host;

    public ConsumerStream(KafkaStream<byte[],byte[]> a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
        host = "localhost";
    }

    
	@Override
	public void run() {
        ConsumerIterator<byte[],byte[]> it = m_stream.iterator();
        while (it.hasNext()) {
        	MessageAndMetadata<byte[],byte[]> next = it.next();
        	byte[] data = next.message();
        	Message message = new Message(data);
            System.out.println("Thread " + m_threadNumber + ": " + message.payload().toString());
            Iterable<List<Object>> tups = _spoutConfig.scheme.deserialize(Utils.toByteArray(message.payload()));
            if(tups!=null) {
                for(List<Object> tup: tups) {
                    collector.emit(tup, new KafkaMessageId(new GlobalPartitionId(new HostPort(host), next.partition()), next.offset()));
                }
            }
        }
        System.out.println("Shutting down Thread: " + m_threadNumber);
	}

}
