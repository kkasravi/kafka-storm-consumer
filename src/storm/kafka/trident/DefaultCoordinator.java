package storm.kafka.trident;

public class DefaultCoordinator implements IBatchCoordinator {

    public boolean isReady(long txid) {
        return true;
    }

    public void close() {
    }
    
}
