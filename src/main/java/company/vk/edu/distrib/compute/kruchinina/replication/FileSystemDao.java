package company.vk.edu.distrib.compute.kruchinina.replication;

import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Base64;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Реализация DAO для хранения данных в файловой системе.
 * Каждый ключ сохраняется в отдельный файл в указанной директории, запись происходит атомарно через временный файл.
 * Потокобезопасна: операции синхронизируются по ключу, временные файлы автоматически очищаются при старте.
 */
public class FileSystemDao implements Dao<byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(FileSystemDao.class);
    private static final String TEMP_SUFFIX = ".tmp";
    private final Path storageDir;
    //Блокировки для предотвращения гонок
    private final Map<String, ReentrantLock> keyLocks = new ConcurrentHashMap<>();

    //Счётчик количества ключей (файлов) в хранилище
    private final AtomicInteger keyCount = new AtomicInteger(0);

    public FileSystemDao(final String storageDirPath) throws IOException {
        this.storageDir = Paths.get(storageDirPath);
        if (!Files.exists(storageDir)) {
            Files.createDirectories(storageDir);
            LOG.info("Created storage directory: {}", storageDir);
        }
        if (!Files.isDirectory(storageDir)) {
            throw new IllegalArgumentException("Path exists but is not a directory: " + storageDirPath);
        }

        //Удалить все временные файлы .tmp при старте (оставшиеся от предыдущих сбоев)
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(storageDir,
                path -> path.toString().endsWith(TEMP_SUFFIX))) {
            for (Path tmpFile : stream) {
                Files.deleteIfExists(tmpFile);
                LOG.debug("Cleaned up stale temporary file: {}", tmpFile);
            }
        }

        //Подсчитать существующие ключи
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(storageDir,
                path -> !path.toString().endsWith(TEMP_SUFFIX))) {
            int count = 0;
            for (Path ignored : stream) {
                count++;
            }
            keyCount.set(count);
        }
    }

    private Path getFileForKey(final String key) {
        String encodedKey = Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(key.getBytes(StandardCharsets.UTF_8));
        return storageDir.resolve(encodedKey);
    }

    private ReentrantLock getLockForKey(String key) {
        return keyLocks.computeIfAbsent(key, k -> new ReentrantLock());
    }

    private void cleanupLock(String key, ReentrantLock lock) {
        if (!lock.isLocked() && !lock.hasQueuedThreads()) {
            keyLocks.remove(key, lock);
        }
    }

    @Override
    public byte[] get(final String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        validateKey(key);
        ReentrantLock lock = getLockForKey(key);
        lock.lock();
        try {
            final Path file = getFileForKey(key);
            if (!Files.exists(file)) {
                throw new NoSuchElementException("No data for key: " + key);
            }
            return Files.readAllBytes(file);
        } finally {
            lock.unlock();
            cleanupLock(key, lock);
        }
    }

    @Override
    public void upsert(final String key, final byte[] value) throws IllegalArgumentException, IOException {
        validateKey(key);
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        ReentrantLock lock = getLockForKey(key);
        lock.lock();
        try {
            final Path target = getFileForKey(key);
            boolean existed = Files.exists(target);
            final Path temp = Files.createTempFile(storageDir, key, TEMP_SUFFIX);
            try {
                Files.write(temp, value);
                Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                Files.deleteIfExists(temp);
                throw e;
            }
            if (!existed) {
                keyCount.incrementAndGet();
            }
            LOG.debug("Upserted key: {}", key);
        } finally {
            lock.unlock();
            cleanupLock(key, lock);
        }
    }

    @Override
    public void delete(final String key) throws IllegalArgumentException, IOException {
        validateKey(key);
        ReentrantLock lock = getLockForKey(key);
        lock.lock();
        try {
            final Path file = getFileForKey(key);
            boolean deleted = Files.deleteIfExists(file);
            if (deleted) {
                keyCount.decrementAndGet();
            }
            LOG.debug("Deleted key: {}", key);
        } finally {
            lock.unlock();
            cleanupLock(key, lock);
        }
    }

    /**
     * Возвращает текущее количество ключей в хранилище.
     */
    public int keyCount() {
        return keyCount.get();
    }

    @Override
    public void close() throws IOException {
        keyLocks.clear();
        LOG.debug("FileSystemDao closed");
    }

    private static void validateKey(final String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must be non-empty");
        }
    }
}
