package company.vk.edu.distrib.compute.kruchinina.consensus;

/**
 * Сообщение ELECT, рассылаемое инициатором выборов узлам с бо́льшими ID.
 */
public class ElectMessage extends Message {
    public ElectMessage(int senderId, long term) {
        super(senderId, term);
    }
}
