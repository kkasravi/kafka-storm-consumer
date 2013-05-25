package storm.kafka;

import backtype.storm.spout.SpoutOutputCollector;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

public class ConsumerStream implements Runnable {

    private KafkaStream<byte[],byte[]> m_stream;
    private int m_threadNumber;
    SpoutOutputCollector collector;

    public ConsumerStream(KafkaStream<byte[],byte[]> a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
    }

    
	@Override
	public void run() {
        ConsumerIterator<byte[],byte[]> it = m_stream.iterator();
        while (it.hasNext()) {
        	MessageAndMetadata<byte[],byte[]> next = it.next();
        	byte[] data = next.message();
        	Message message = new Message(data);
            System.out.println("Thread " + m_threadNumber + ": " + message.payload().toString());
        }
        System.out.println("Shutting down Thread: " + m_threadNumber);
	}

}
