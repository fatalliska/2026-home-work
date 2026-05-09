package company.vk.edu.distrib.compute.kruchinina.consensus;

import java.util.*;
import java.util.concurrent.*;

/**
 * Модель узла распределённой системы.
 * Каждый узел работает в собственном потоке, поддерживает очередь входящих сообщений
 * (BlockingQueue) и реализует алгоритм выбора лидера Bully.
 */
public class ClusterNode extends Thread {
    public enum State { FOLLOWER, CANDIDATE, LEADER }

    private final int nodeId;
    private final Map<Integer, ClusterNode> allNodes;
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();
    private volatile State state = State.FOLLOWER;
    private volatile int leaderId = -1;
    private volatile long currentTerm = 0;
    private volatile boolean failed = false;
    private volatile boolean shuttingDown = false;
    private final Random random = new Random();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private ScheduledFuture<?> pingTask;
    private ScheduledFuture<?> electionTask;
    private ScheduledFuture<?> pingTimeoutTask;
    private ScheduledFuture<?> failureCheckTask;

    private static final long PING_INTERVAL = 1500;
    private static final long PING_TIMEOUT = 2000;
    private final long ELECTION_TIMEOUT = 2000 + random.nextInt(1000);
    private static final double FAILURE_PROBABILITY = 0.05;
    private static final long FAILURE_CHECK_INTERVAL = 2000;

    public ClusterNode(int nodeId, Map<Integer, ClusterNode> allNodes) {
        this.nodeId = nodeId;
        this.allNodes = allNodes;
    }

    public void receiveMessage(Message msg) {
        if (!failed && !shuttingDown) {
            inbox.offer(msg);
        }
    }

    public void fail() {
        synchronized (this) {
            if (failed) return;
            failed = true;
            System.out.printf("[Node %d] Forced failure%n", nodeId);
            cancelElectionAndPingTimers();
            state = State.FOLLOWER;
            leaderId = -1;
        }
    }

    public void recover() {
        synchronized (this) {
            if (!failed) return;
            failed = false;
            System.out.printf("[Node %d] Recovered%n", nodeId);
            leaderId = -1;
            becomeCandidate();
        }
    }

    /** Плавное отключение лидера с оповещением кластера. */
    public void gracefulShutdown() {
        synchronized (this) {
            if (state != State.LEADER || shuttingDown) return;
            shuttingDown = true;
            System.out.printf("[Node %d] Graceful shutdown as leader%n", nodeId);
            broadcast(new VictoryMessage(nodeId, currentTerm, -1));
        }
        try {
            Thread.sleep(500);
        } catch (InterruptedException ignored) {}
        this.interrupt();
    }

    @Override
    public void run() {
        failureCheckTask = scheduler.scheduleAtFixedRate(
                this::checkRandomFailure,
                FAILURE_CHECK_INTERVAL, FAILURE_CHECK_INTERVAL, TimeUnit.MILLISECONDS);

        scheduler.schedule(() -> {
            synchronized (ClusterNode.this) {
                if (!failed && state == State.FOLLOWER && leaderId == -1) {
                    becomeCandidate();
                }
            }
        }, random.nextInt(500), TimeUnit.MILLISECONDS);

        while (!Thread.interrupted() && !shuttingDown) {
            try {
                Message msg = inbox.poll(500, TimeUnit.MILLISECONDS);
                if (msg != null && !failed && !shuttingDown) {
                    handleMessage(msg);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        cancelElectionAndPingTimers();
        if (failureCheckTask != null) failureCheckTask.cancel(false);
        scheduler.shutdownNow();
        System.out.printf("[Node %d] Thread exiting%n", nodeId);
    }

    private void checkRandomFailure() {
        if (failed || shuttingDown) return;
        if (random.nextDouble() < FAILURE_PROBABILITY) {
            fail();
        }
    }

    private void handleMessage(Message msg) {
        synchronized (this) {
            if (msg.term < currentTerm) return;

            if (msg.term > currentTerm) {
                currentTerm = msg.term;
                state = State.FOLLOWER;
                leaderId = -1;
                cancelElectionAndPingTimers();
            }

            if (msg instanceof PingMessage)           handlePing((PingMessage) msg);
            else if (msg instanceof ElectMessage)     handleElect((ElectMessage) msg);
            else if (msg instanceof AnswerMessage)    handleAnswer((AnswerMessage) msg);
            else if (msg instanceof VictoryMessage)   handleVictory((VictoryMessage) msg);
        }
    }

    private void handlePing(PingMessage msg) {
        if (state == State.LEADER) {
            send(new AnswerMessage(nodeId, currentTerm), msg.senderId);
        }
    }

    private void handleElect(ElectMessage msg) {
        if (nodeId > msg.senderId) {
            send(new AnswerMessage(nodeId, currentTerm), msg.senderId);
            if (state != State.CANDIDATE && state != State.LEADER) {
                becomeCandidate();
            }
        }
    }

    private void handleAnswer(AnswerMessage msg) {
        if (state == State.CANDIDATE) {
            state = State.FOLLOWER;
            leaderId = msg.senderId;
            cancelElectionAndPingTimers();
            startPingTimer();
        }
    }

    private void handleVictory(VictoryMessage msg) {
        if (msg.leaderId == -1) {
            if (leaderId == msg.senderId || leaderId == -1) {
                System.out.printf("[Node %d] Leader %d resigned, starting election%n", nodeId, msg.senderId);
                leaderId = -1;
                state = State.FOLLOWER;
                cancelElectionAndPingTimers();
                becomeCandidate();
            }
        } else {
            state = State.FOLLOWER;
            leaderId = msg.leaderId;
            cancelElectionAndPingTimers();
            startPingTimer();
            System.out.printf("[Node %d] New leader is %d%n", nodeId, leaderId);
        }
    }

    private void becomeCandidate() {
        state = State.CANDIDATE;
        currentTerm++;
        leaderId = -1;
        System.out.printf("[Node %d] Starting election (term=%d)%n", nodeId, currentTerm);

        for (ClusterNode node : allNodes.values()) {
            if (node.nodeId > this.nodeId && !node.failed) {
                send(new ElectMessage(nodeId, currentTerm), node.nodeId);
            }
        }

        cancelElectionTimer();
        electionTask = scheduler.schedule(() -> {
            synchronized (ClusterNode.this) {
                if (state == State.CANDIDATE) {
                    state = State.LEADER;
                    leaderId = nodeId;
                    System.out.printf("[Node %d] Elected as leader (term=%d)%n", nodeId, currentTerm);
                    broadcast(new VictoryMessage(nodeId, currentTerm, nodeId));
                    cancelElectionAndPingTimers();
                }
            }
        }, ELECTION_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    private void startPingTimer() {
        cancelPingTimer();
        pingTask = scheduler.scheduleAtFixedRate(() -> {
            synchronized (ClusterNode.this) {
                if (state == State.FOLLOWER && leaderId != -1 && leaderId != nodeId && !failed) {
                    send(new PingMessage(nodeId, currentTerm), leaderId);
                    schedulePingTimeout();
                }
            }
        }, 0, PING_INTERVAL, TimeUnit.MILLISECONDS);
    }

    private void schedulePingTimeout() {
        cancelPingTimeoutTask();
        pingTimeoutTask = scheduler.schedule(() -> {
            synchronized (ClusterNode.this) {
                if (state == State.FOLLOWER && leaderId != -1 && !failed) {
                    System.out.printf("[Node %d] Ping timeout, leader %d assumed dead%n", nodeId, leaderId);
                    leaderId = -1;
                    becomeCandidate();
                }
            }
        }, PING_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    private void cancelPingTimer() {
        if (pingTask != null) { pingTask.cancel(false); pingTask = null; }
    }

    private void cancelPingTimeoutTask() {
        if (pingTimeoutTask != null) { pingTimeoutTask.cancel(false); pingTimeoutTask = null; }
    }

    private void cancelElectionTimer() {
        if (electionTask != null) { electionTask.cancel(false); electionTask = null; }
    }

    private void cancelElectionAndPingTimers() {
        cancelPingTimer();
        cancelPingTimeoutTask();
        cancelElectionTimer();
    }

    private void send(Message msg, int targetId) {
        ClusterNode target = allNodes.get(targetId);
        if (target != null && target != this && !target.failed) {
            target.receiveMessage(msg);
        }
    }

    private void broadcast(Message msg) {
        for (ClusterNode node : allNodes.values()) {
            if (node != this && !node.failed) {
                node.receiveMessage(msg);
            }
        }
    }

    public int getNodeId() { return nodeId; }
    public State getNodeState() { return state; }
    public int getLeaderId() { return leaderId; }
    public long getCurrentTerm() { return currentTerm; }
    public boolean isFailed() { return failed; }
}
