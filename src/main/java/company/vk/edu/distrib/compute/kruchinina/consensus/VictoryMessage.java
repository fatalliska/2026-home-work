package company.vk.edu.distrib.compute.kruchinina.consensus;

/**
 * Поле leaderId = -1 используется для планового ухода лидера (graceful shutdown).
 */
public class VictoryMessage extends Message {
    public final int leaderId;

    public VictoryMessage(int senderId, long term, int leaderId) {
        super(senderId, term);
        this.leaderId = leaderId;
    }
}
