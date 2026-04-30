package company.vk.edu.distrib.compute.kruchinina.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class GrpcClusterClient {

    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    public byte[] get(String extendedNode, String key, int ack) throws IOException {
        KeyValueInternalServiceGrpc.KeyValueInternalServiceBlockingStub stub = createStub(extendedNode);
        try {
            KvInternalProto.GetResponse resp = stub.get(KvInternalProto.GetRequest.newBuilder()
                    .setKey(key)
                    .setAck(ack)
                    .build());
            return resp.getData().toByteArray();
        } catch (StatusRuntimeException e) {
            throwConvertedException(e);
            return new byte[0];
        }
    }

    public void upsert(String extendedNode, String key, byte[] value, int ack) throws IOException {
        KeyValueInternalServiceGrpc.KeyValueInternalServiceBlockingStub stub = createStub(extendedNode);
        try {
            stub.upsert(KvInternalProto.UpsertRequest.newBuilder()
                    .setKey(key)
                    .setValue(com.google.protobuf.ByteString.copyFrom(value))
                    .setAck(ack)
                    .build());
        } catch (StatusRuntimeException e) {
            throwConvertedException(e);
        }
    }

    public void delete(String extendedNode, String key, int ack) throws IOException {
        KeyValueInternalServiceGrpc.KeyValueInternalServiceBlockingStub stub = createStub(extendedNode);
        try {
            stub.delete(KvInternalProto.DeleteRequest.newBuilder()
                    .setKey(key)
                    .setAck(ack)
                    .build());
        } catch (StatusRuntimeException e) {
            throwConvertedException(e);
        }
    }

    private KeyValueInternalServiceGrpc.KeyValueInternalServiceBlockingStub createStub(String extendedNode) {
        int grpcPort = extractGrpcPort(extendedNode);
        String target = "localhost:" + grpcPort;
        ManagedChannel channel = channels.computeIfAbsent(target,
                t -> ManagedChannelBuilder.forTarget(t).usePlaintext().build());
        return KeyValueInternalServiceGrpc.newBlockingStub(channel);
    }

    private int extractGrpcPort(String extendedNode) {
        int idx = extendedNode.indexOf("?grpcPort=");
        if (idx == -1) {
            throw new IllegalArgumentException("Missing grpcPort in node address: " + extendedNode);
        }
        return Integer.parseInt(extendedNode.substring(idx + "?grpcPort=".length()));
    }

    //Преобразует gRPC-ошибку в проверяемое исключение
    private void throwConvertedException(StatusRuntimeException e) throws IOException {
        Status status = e.getStatus();
        String desc = status.getDescription();
        switch (status.getCode()) {
            case NOT_FOUND:
                throw new NoSuchElementException(desc);
            case INVALID_ARGUMENT:
                throw new IllegalArgumentException(desc);
            default:
                throw new IOException(desc, e);
        }
    }

    public void shutdown() {
        channels.values().forEach(ch -> {
            ch.shutdown();
            try {
                if (!ch.awaitTermination(1, TimeUnit.SECONDS)) {
                    ch.shutdownNow();
                }
            } catch (InterruptedException ex) {
                ch.shutdownNow();
                Thread.currentThread().interrupt();
            }
        });
        channels.clear();
    }
}
