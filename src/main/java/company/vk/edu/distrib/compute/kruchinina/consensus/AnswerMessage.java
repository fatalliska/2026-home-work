package company.vk.edu.distrib.compute.kruchinina.consensus;

public class AnswerMessage extends Message {
    public AnswerMessage(int senderId, long term) {
        super(senderId, term);
    }
}
