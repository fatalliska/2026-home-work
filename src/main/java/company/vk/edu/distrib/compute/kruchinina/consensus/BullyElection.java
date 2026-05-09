package company.vk.edu.distrib.compute.kruchinina.consensus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Демонстрация работы алгоритма выбора лидера.
 */
public final class BullyElection {

    private static final Logger LOG = Logger.getLogger(BullyElection.class.getName());

    private BullyElection() {
        //utility class
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops") //т.к. необходимо создание нескольких одинаковых узлов
    static void main(String[] args) throws InterruptedException {
        final int nodeCount = 5;
        Map<Integer, ClusterNode> nodes = new ConcurrentHashMap<>();

        for (int i = 1; i <= nodeCount; i++) {
            nodes.put(i, new ClusterNode(i, nodes));
        }
        for (ClusterNode node : nodes.values()) {
            node.start();
        }

        Thread.sleep(5000);
        LOG.info("=== Initial state ===");
        printStatus(nodes);

        LOG.info("=== Forcing failure of node 3 ===");
        nodes.get(3).fail();
        Thread.sleep(6000);
        printStatus(nodes);

        LOG.info("=== Recovering node 3 ===");
        nodes.get(3).recover();
        Thread.sleep(5000);
        printStatus(nodes);

        ClusterNode leader = nodes.values().stream()
                .filter(n -> n.getNodeState() == ClusterNode.State.LEADER)
                .findFirst().orElse(null);
        if (leader != null) {
            LOG.info(() -> String.format("=== Graceful shutdown of leader %d ===", leader.getNodeId()));
            leader.gracefulShutdown();
            Thread.sleep(5000);
            printStatus(nodes);
        }

        for (ClusterNode node : nodes.values()) {
            node.interrupt();
        }
        LOG.info("Finished.");
    }

    private static void printStatus(Map<Integer, ClusterNode> nodes) {
        for (ClusterNode n : nodes.values()) {
            LOG.info(() -> String.format("Node %d: state=%s, leader=%d, term=%d, failed=%b",
                    n.getNodeId(), n.getNodeState(), n.getLeaderId(), n.getCurrentTerm(), n.isFailed()));
        }
    }
}
