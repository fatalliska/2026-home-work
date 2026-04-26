package company.vk.edu.distrib.compute.kruchinina.replication;

import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ReplicatedFileSystemDao implements Dao<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicatedFileSystemDao.class);

    private final int replicaCount;
    private final int defaultAck;
    private final List<FileSystemDao> replicas;
    private final ExecutorService executor;
    private final boolean[] enabled;
    private final AtomicInteger[] readCounters;
    private final AtomicInteger[] writeCounters;

    public ReplicatedFileSystemDao(String baseDir, int n) throws IOException {
        if (n < 1) {
            throw new IllegalArgumentException("Number of replicas must be at least 1");
        }
        this.replicaCount = n;
        this.defaultAck = 1;
        this.replicas = new ArrayList<>(n);
        this.enabled = new boolean[n];
        this.readCounters = new AtomicInteger[n];
        this.writeCounters = new AtomicInteger[n];
        this.executor = Executors.newFixedThreadPool(n);

        for (int i = 0; i < n; i++) {
            Path replicaDir = Path.of(baseDir + "_r" + i);
            Files.createDirectories(replicaDir);
            replicas.add(new FileSystemDao(replicaDir.toString()));
            enabled[i] = true;
            readCounters[i] = new AtomicInteger(0);
            writeCounters[i] = new AtomicInteger(0);
        }
    }

    @Override
    public byte[] get(String key) throws IOException, NoSuchElementException {
        return get(key, defaultAck);
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        upsert(key, value, defaultAck);
    }

    @Override
    public void delete(String key) throws IOException {
        delete(key, defaultAck);
    }

    public byte[] get(String key, int ack) throws IOException {
        validateAck(ack);
        byte[] result = readFromReplicas(key, ack);
        if (result == null) {
            throw new IOException("Not enough replicas available (need " + ack + ")");
        }
        return result;
    }

    private byte[] readFromReplicas(String key, int ack) {
        List<CompletableFuture<byte[]>> futures = submitReadTasks(key);
        return collectReadResults(futures, ack);
    }

    private List<CompletableFuture<byte[]>> submitReadTasks(String key) {
        List<CompletableFuture<byte[]>> futures = new ArrayList<>();
        for (int i = 0; i < replicaCount; i++) {
            final int idx = i;
            if (enabled[idx]) {
                futures.add(CompletableFuture.supplyAsync(() -> {
                    try {
                        byte[] data = replicas.get(idx).get(key);
                        readCounters[idx].incrementAndGet();
                        return data;
                    } catch (IOException e) {
                        return null;
                    }
                }, executor));
            }
        }
        return futures;
    }

    private enum FutureResult { SUCCESS, NOT_FOUND, TIMEOUT_OR_ERROR }

    private FutureResult getFutureResult(CompletableFuture<byte[]> future,
                                         AtomicReference<byte[]> dataHolder) {
        try {
            byte[] data = future.get(500, TimeUnit.MILLISECONDS);
            dataHolder.set(data);
            return FutureResult.SUCCESS;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof NoSuchElementException) {
                return FutureResult.NOT_FOUND;
            }
            return FutureResult.TIMEOUT_OR_ERROR;
        } catch (TimeoutException e) {
            return FutureResult.TIMEOUT_OR_ERROR;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return FutureResult.TIMEOUT_OR_ERROR;
        }
    }

    private byte[] collectReadResults(List<CompletableFuture<byte[]>> futures, int ack) {
        int success = 0;
        byte[] firstData = null;
        int notFoundCount = 0;

        for (CompletableFuture<byte[]> future : futures) {
            AtomicReference<byte[]> dataRef = new AtomicReference<>();
            FutureResult result = getFutureResult(future, dataRef);

            switch (result) {
                case SUCCESS:
                    success++;
                    if (firstData == null) {
                        firstData = dataRef.get();
                    }
                    break;
                case NOT_FOUND:
                    notFoundCount++;
                    break;
                default:
                    //Таймаут или ошибка – реплика недоступна
                    break;
            }

            if (success >= ack) {
                break;
            }
        }

        if (success >= ack) {
            return firstData;
        }
        if (success == 0 && notFoundCount == futures.size()) {
            throw new NoSuchElementException("No data for key");
        }
        return null;
    }

    public void upsert(String key, byte[] value, int ack) throws IOException {
        validateAck(ack);
        CountDownLatch latch = new CountDownLatch(replicaCount);
        AtomicInteger successCount = new AtomicInteger(0);
        for (int i = 0; i < replicaCount; i++) {
            final int idx = i;
            if (!enabled[idx]) {
                latch.countDown();
                continue;
            }
            executor.submit(() -> {
                try {
                    replicas.get(idx).upsert(key, value);
                    successCount.incrementAndGet();
                    writeCounters[idx].incrementAndGet();
                } catch (IOException e) {
                    LOG.warn("Replica {} failed to upsert key {}: {}", idx, key, e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        awaitLatch(latch);
        if (successCount.get() < ack) {
            throw new IOException("Upsert failed: only " + successCount.get() + " replicas acknowledged, need " + ack);
        }
    }

    public void delete(String key, int ack) throws IOException {
        validateAck(ack);
        CountDownLatch latch = new CountDownLatch(replicaCount);
        AtomicInteger successCount = new AtomicInteger(0);
        for (int i = 0; i < replicaCount; i++) {
            final int idx = i;
            if (!enabled[idx]) {
                latch.countDown();
                continue;
            }
            executor.submit(() -> {
                try {
                    replicas.get(idx).delete(key);
                    successCount.incrementAndGet();
                } catch (IOException e) {
                    LOG.warn("Replica {} failed to delete key {}: {}", idx, key, e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        awaitLatch(latch);
        if (successCount.get() < ack) {
            throw new IOException("Delete failed: only " + successCount.get() + " replicas acknowledged, need " + ack);
        }
    }

    private void validateAck(int ack) {
        if (ack > replicaCount || ack < 1) {
            throw new IllegalArgumentException("Invalid ack: " + ack + " (must be 1.." + replicaCount + ")");
        }
    }

    private void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                LOG.warn("Timeout waiting for replica operations");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void disableReplica(int nodeId) {
        if (nodeId < 0 || nodeId >= replicaCount) {
            throw new IllegalArgumentException("Invalid replica index: " + nodeId);
        }
        enabled[nodeId] = false;
    }

    public void enableReplica(int nodeId) {
        if (nodeId < 0 || nodeId >= replicaCount) {
            throw new IllegalArgumentException("Invalid replica index: " + nodeId);
        }
        enabled[nodeId] = true;
    }

    public int getReplicaCount() {
        return replicaCount;
    }

    @Override
    public void close() throws IOException {
        executor.shutdownNow();
        for (FileSystemDao dao : replicas) {
            dao.close();
        }
    }

    public int getKeyCount(int replicaIndex) {
        return replicas.get(replicaIndex).keyCount();
    }

    public int getReadAccessCount(int replicaIndex) {
        return readCounters[replicaIndex].get();
    }

    public int getWriteAccessCount(int replicaIndex) {
        return writeCounters[replicaIndex].get();
    }
}
