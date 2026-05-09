package company.vk.edu.distrib.compute.kruchinina.consensus;

public class ElectMessage extends Message {
    public ElectMessage(int senderId, long term) {
        super(senderId, term);
    }
}
