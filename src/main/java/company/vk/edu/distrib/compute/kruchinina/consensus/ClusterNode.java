package company.vk.edu.distrib.compute.kruchinina.consensus;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 * Представление узла распределённой системы.
 * Каждый узел работает в собственном потоке, поддерживает очередь входящих сообщений
 * (BlockingQueue) и реализует алгоритм выбора лидера Bully.
 */
public class ClusterNode extends Thread {
    public enum State { FOLLOWER, CANDIDATE, LEADER }

    private static final Logger LOG = Logger.getLogger(ClusterNode.class.getName());

    private final int nodeId;
    private final Map<Integer, ClusterNode> allNodes;
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();

    private final AtomicReference<State> state = new AtomicReference<>(State.FOLLOWER);
    private final AtomicInteger leaderId = new AtomicInteger(-1);
    private final AtomicLong currentTerm = new AtomicLong(0);
    private final AtomicBoolean failed = new AtomicBoolean(false);
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    private final Random random = new Random();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final ReentrantLock lock = new ReentrantLock();

    private ScheduledFuture<?> pingTask;
    private ScheduledFuture<?> electionTask;
    private ScheduledFuture<?> pingTimeoutTask;

    private static final long PING_INTERVAL_MS = 1500L;
    private static final long PING_TIMEOUT_MS = 2000L;
    private final long electionTimeoutMs = 2000L + random.nextInt(1000);
    private static final double FAILURE_PROBABILITY = 0.05;
    private static final long FAILURE_CHECK_INTERVAL_MS = 2000L;

    public ClusterNode(int nodeId, Map<Integer, ClusterNode> allNodes) {
        super(); // убирает предупреждение о вызове super
        this.nodeId = nodeId;
        this.allNodes = allNodes;
    }

    public void receiveMessage(Message msg) {
        if (!failed.get() && !shuttingDown.get()) {
            inbox.offer(msg);
        }
    }

    public void fail() {
        lock.lock();
        try {
            if (failed.get()) {
                return;
            }
            failed.set(true);
            LOG.info(() -> String.format("[Node %d] Forced failure", nodeId));
            cancelElectionAndPingTimersInsideLock();
            state.set(State.FOLLOWER);
            leaderId.set(-1);
        } finally {
            lock.unlock();
        }
    }

    public void recover() {
        lock.lock();
        try {
            if (!failed.get()) {
                return;
            }
            failed.set(false);
            LOG.info(() -> String.format("[Node %d] Recovered", nodeId));
            leaderId.set(-1);
            becomeCandidateInsideLock();
        } finally {
            lock.unlock();
        }
    }

    /** Плавное отключение лидера с оповещением кластера. */
    public void gracefulShutdown() {
        lock.lock();
        try {
            if (state.get() != State.LEADER || shuttingDown.get()) {
                return;
            }
            shuttingDown.set(true);
            LOG.info(() -> String.format("[Node %d] Graceful shutdown as leader", nodeId));
            broadcast(new VictoryMessage(nodeId, currentTerm.get(), -1));
        } finally {
            lock.unlock();
        }
        try {
            sleep(500);
        } catch (InterruptedException e) {
            //Завершаем работу, игнорируем
        }
        this.interrupt();
    }

    @Override
    public void run() {
        startFailureCheck();
        scheduleInitialElection();
        runMessageLoop();
        cleanup();
    }

    private void startFailureCheck() {
        scheduler.scheduleAtFixedRate(
                this::checkRandomFailure,
                FAILURE_CHECK_INTERVAL_MS, FAILURE_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void scheduleInitialElection() {
        scheduler.schedule(() -> {
            lock.lock();
            try {
                if (!failed.get() && state.get() == State.FOLLOWER && leaderId.get() == -1) {
                    becomeCandidateInsideLock();
                }
            } finally {
                lock.unlock();
            }
        }, random.nextInt(500), TimeUnit.MILLISECONDS);
    }

    private void runMessageLoop() {
        while (!isInterrupted() && !shuttingDown.get()) {
            try {
                Message msg = inbox.poll(500, TimeUnit.MILLISECONDS);
                if (msg != null && !failed.get() && !shuttingDown.get()) {
                    handleMessage(msg);
                }
            } catch (InterruptedException e) {
                currentThread().interrupt();
                break;
            }
        }
    }

    private void cleanup() {
        lock.lock();
        try {
            cancelElectionAndPingTimersInsideLock();
        } finally {
            lock.unlock();
        }
        scheduler.shutdownNow();
        LOG.info(() -> String.format("[Node %d] Thread exiting", nodeId));
    }

    private void checkRandomFailure() {
        if (failed.get() || shuttingDown.get()) {
            return;
        }
        if (random.nextDouble() < FAILURE_PROBABILITY) {
            fail();
        }
    }

    private void handleMessage(Message msg) {
        lock.lock();
        try {
            if (msg.term < currentTerm.get()) {
                return;
            }
            if (msg.term > currentTerm.get()) {
                currentTerm.set(msg.term);
                state.set(State.FOLLOWER);
                leaderId.set(-1);
                cancelElectionAndPingTimersInsideLock();
            }
            if (msg instanceof PingMessage) {
                handlePingInsideLock((PingMessage) msg);
            } else if (msg instanceof ElectMessage) {
                handleElectInsideLock((ElectMessage) msg);
            } else if (msg instanceof AnswerMessage) {
                handleAnswerInsideLock((AnswerMessage) msg);
            } else if (msg instanceof VictoryMessage) {
                handleVictoryInsideLock((VictoryMessage) msg);
            }
        } finally {
            lock.unlock();
        }
    }

    private void handlePingInsideLock(PingMessage msg) {
        if (state.get() == State.LEADER) {
            send(new AnswerMessage(nodeId, currentTerm.get()), msg.senderId);
        }
    }

    private void handleElectInsideLock(ElectMessage msg) {
        if (nodeId > msg.senderId) {
            send(new AnswerMessage(nodeId, currentTerm.get()), msg.senderId);
            if (state.get() != State.CANDIDATE && state.get() != State.LEADER) {
                becomeCandidateInsideLock();
            }
        }
    }

    private void handleAnswerInsideLock(AnswerMessage msg) {
        if (state.get() == State.CANDIDATE) {
            state.set(State.FOLLOWER);
            leaderId.set(msg.senderId);
            cancelElectionAndPingTimersInsideLock();
            startPingTimerInsideLock();
        }
    }

    private void handleVictoryInsideLock(VictoryMessage msg) {
        if (msg.leaderId == -1) {
            if (leaderId.get() == msg.senderId || leaderId.get() == -1) {
                LOG.info(() -> String.format("[Node %d] Leader %d resigned, starting election",
                        nodeId, msg.senderId));
                leaderId.set(-1);
                state.set(State.FOLLOWER);
                cancelElectionAndPingTimersInsideLock();
                becomeCandidateInsideLock();
            }
        } else {
            state.set(State.FOLLOWER);
            leaderId.set(msg.leaderId);
            cancelElectionAndPingTimersInsideLock();
            startPingTimerInsideLock();
            LOG.info(() -> String.format("[Node %d] New leader is %d", nodeId, leaderId.get()));
        }
    }

    private void becomeCandidateInsideLock() {
        state.set(State.CANDIDATE);
        currentTerm.incrementAndGet();
        leaderId.set(-1);
        LOG.info(() -> String.format("[Node %d] Starting election (term=%d)", nodeId, currentTerm.get()));

        ElectMessage elect = new ElectMessage(nodeId, currentTerm.get());
        for (ClusterNode node : allNodes.values()) {
            if (node.nodeId > this.nodeId && !node.failed.get()) {
                send(elect, node.nodeId);
            }
        }

        cancelElectionTimerInsideLock();
        electionTask = scheduler.schedule(() -> {
            lock.lock();
            try {
                if (state.get() == State.CANDIDATE) {
                    state.set(State.LEADER);
                    leaderId.set(nodeId);
                    LOG.info(() -> String.format("[Node %d] Elected as leader (term=%d)",
                            nodeId, currentTerm.get()));
                    broadcast(new VictoryMessage(nodeId, currentTerm.get(), nodeId));
                    cancelElectionAndPingTimersInsideLock();
                }
            } finally {
                lock.unlock();
            }
        }, electionTimeoutMs, TimeUnit.MILLISECONDS);
    }

    private void startPingTimerInsideLock() {
        cancelPingTimerInsideLock();
        pingTask = scheduler.scheduleAtFixedRate(() -> {
            lock.lock();
            try {
                if (state.get() == State.FOLLOWER && leaderId.get() != -1
                        && leaderId.get() != nodeId && !failed.get()) {
                    send(new PingMessage(nodeId, currentTerm.get()), leaderId.get());
                    schedulePingTimeoutInsideLock();
                }
            } finally {
                lock.unlock();
            }
        }, 0, PING_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void schedulePingTimeoutInsideLock() {
        cancelPingTimeoutTaskInsideLock();
        pingTimeoutTask = scheduler.schedule(() -> {
            lock.lock();
            try {
                if (state.get() == State.FOLLOWER && leaderId.get() != -1 && !failed.get()) {
                    LOG.info(() -> String.format("[Node %d] Ping timeout, leader %d assumed dead",
                            nodeId, leaderId.get()));
                    leaderId.set(-1);
                    becomeCandidateInsideLock();
                }
            } finally {
                lock.unlock();
            }
        }, PING_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private void cancelPingTimerInsideLock() {
        if (pingTask != null) {
            pingTask.cancel(false);
        }
    }

    private void cancelPingTimeoutTaskInsideLock() {
        if (pingTimeoutTask != null) {
            pingTimeoutTask.cancel(false);
        }
    }

    private void cancelElectionTimerInsideLock() {
        if (electionTask != null) {
            electionTask.cancel(false);
        }
    }

    private void cancelElectionAndPingTimersInsideLock() {
        cancelPingTimerInsideLock();
        cancelPingTimeoutTaskInsideLock();
        cancelElectionTimerInsideLock();
    }

    private void send(Message msg, int targetId) {
        ClusterNode target = allNodes.get(targetId);
        if (target != null && target != this && !target.failed.get()) {
            target.receiveMessage(msg);
        }
    }

    private void broadcast(Message msg) {
        for (ClusterNode node : allNodes.values()) {
            if (node != this && !node.failed.get()) {
                node.receiveMessage(msg);
            }
        }
    }

    public int getNodeId() {
        return nodeId;
    }

    public State getNodeState() {
        return state.get();
    }

    public int getLeaderId() {
        return leaderId.get();
    }

    public long getCurrentTerm() {
        return currentTerm.get();
    }

    public boolean isFailed() {
        return failed.get();
    }
}
