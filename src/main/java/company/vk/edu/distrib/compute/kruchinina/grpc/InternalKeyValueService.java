package company.vk.edu.distrib.compute.kruchinina.grpc;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kruchinina.replication.ReplicatedFileSystemDao;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.NoSuchElementException;

public class InternalKeyValueService extends KeyValueInternalServiceGrpc.KeyValueInternalServiceImplBase {

    private final Dao<byte[]> dao;

    public InternalKeyValueService(Dao<byte[]> dao) {
        super(); // явный вызов, убирает замечание CallSuperInConstructor
        this.dao = dao;
    }

    @Override
    public void get(KvInternalProto.GetRequest request,
                    StreamObserver<KvInternalProto.GetResponse> responseObserver) {
        // Проверка ack до try – исключение не будет лететь внутри try и ловиться
        if (!(dao instanceof ReplicatedFileSystemDao) && request.getAck() != 1) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Replication not supported")
                    .asException());
            return;
        }

        try {
            byte[] data;
            if (dao instanceof ReplicatedFileSystemDao) {
                data = ((ReplicatedFileSystemDao) dao).get(request.getKey(), request.getAck());
            } else {
                data = dao.get(request.getKey());
            }
            responseObserver.onNext(KvInternalProto.GetResponse.newBuilder()
                    .setData(com.google.protobuf.ByteString.copyFrom(data))
                    .build());
            responseObserver.onCompleted();
        } catch (NoSuchElementException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
        } catch (IOException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void upsert(KvInternalProto.UpsertRequest request,
                       StreamObserver<KvInternalProto.UpsertResponse> responseObserver) {
        if (!(dao instanceof ReplicatedFileSystemDao) && request.getAck() != 1) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Replication not supported")
                    .asException());
            return;
        }

        try {
            if (dao instanceof ReplicatedFileSystemDao) {
                ((ReplicatedFileSystemDao) dao).upsert(
                        request.getKey(),
                        request.getValue().toByteArray(),
                        request.getAck());
            } else {
                dao.upsert(request.getKey(), request.getValue().toByteArray());
            }
            responseObserver.onNext(KvInternalProto.UpsertResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (IOException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void delete(KvInternalProto.DeleteRequest request,
                       StreamObserver<KvInternalProto.DeleteResponse> responseObserver) {
        if (!(dao instanceof ReplicatedFileSystemDao) && request.getAck() != 1) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Replication not supported")
                    .asException());
            return;
        }

        try {
            if (dao instanceof ReplicatedFileSystemDao) {
                ((ReplicatedFileSystemDao) dao).delete(request.getKey(), request.getAck());
            } else {
                dao.delete(request.getKey());
            }
            responseObserver.onNext(KvInternalProto.DeleteResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (IOException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }
}
