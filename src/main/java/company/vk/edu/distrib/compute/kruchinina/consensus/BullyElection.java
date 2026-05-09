package company.vk.edu.distrib.compute.kruchinina.consensus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Демонстрация работы алгоритма выбора лидера.
 * Создаёт N узлов, запускает их, затем принудительно отказывает узлу 3,
 * восстанавливает его и плавно отключает текущего лидера.
 */
public class BullyElection {
    public static void main(String[] args) throws InterruptedException {
        final int N = 5;
        Map<Integer, ClusterNode> nodes = new ConcurrentHashMap<>();

        //Создание узлов
        for (int i = 1; i <= N; i++) {
            nodes.put(i, new ClusterNode(i, nodes));
        }

        //Запуск потоков
        for (ClusterNode node : nodes.values()) {
            node.start();
        }

        Thread.sleep(5000);
        System.out.println("\n=== Initial state ===");
        printStatus(nodes);

        //Принудительный отказ узла 3
        System.out.println("\n=== Forcing failure of node 3 ===");
        nodes.get(3).fail();
        Thread.sleep(6000);
        printStatus(nodes);

        //Восстановление узла 3
        System.out.println("\n=== Recovering node 3 ===");
        nodes.get(3).recover();
        Thread.sleep(5000);
        printStatus(nodes);

        //Плавное отключение текущего лидера
        ClusterNode leader = nodes.values().stream()
                .filter(n -> n.getNodeState() == ClusterNode.State.LEADER)
                .findFirst().orElse(null);
        if (leader != null) {
            System.out.printf("%n=== Graceful shutdown of leader %d ===%n", leader.getNodeId());
            leader.gracefulShutdown();
            Thread.sleep(5000);
            printStatus(nodes);
        }

        // авершение всех потоков
        for (ClusterNode node : nodes.values()) {
            node.interrupt();
        }
        System.out.println("\nFinished.");
    }

    private static void printStatus(Map<Integer, ClusterNode> nodes) {
        for (ClusterNode n : nodes.values()) {
            System.out.printf("Node %d: state=%s, leader=%d, term=%d, failed=%b%n",
                    n.getNodeId(), n.getNodeState(), n.getLeaderId(), n.getCurrentTerm(), n.isFailed());
        }
    }
}
