package company.vk.edu.distrib.compute.kruchinina.consensus;

import java.util.Objects;

/**
 * Базовый класс для всех сообщений, которыми обмениваются узлы.
 * Каждое сообщение содержит идентификатор отправителя
 * и номер эпохи для обеспечения идемпотентности.
 */
public abstract class Message {
    public final int senderId;
    public final long term;

    protected Message(int senderId, long term) {
        this.senderId = senderId;
        this.term = term;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Message)) return false;
        Message m = (Message) o;
        return senderId == m.senderId && term == m.term;
    }

    @Override
    public int hashCode() {
        return Objects.hash(senderId, term);
    }
}
