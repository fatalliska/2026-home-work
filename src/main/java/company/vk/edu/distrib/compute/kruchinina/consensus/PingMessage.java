package company.vk.edu.distrib.compute.kruchinina.consensus;

public class PingMessage extends Message {
    public PingMessage(int senderId, long term) {
        super(senderId, term);
    }
}
