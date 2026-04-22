package company.vk.edu.distrib.compute.shuuuurik;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

public class FileDao implements Dao<byte[]> {

    private final Path rootDir;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public FileDao(Path rootDir) throws IOException {
        if (rootDir == null) {
            throw new IllegalArgumentException("rootDir must not be null");
        }

        this.rootDir = rootDir;
        Files.createDirectories(rootDir);
    }

    private static void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key must not be null or empty");
        }
    }

    private static void validateValue(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("value must not be null");
        }
    }

    private static String encodeKey(String key) {
        return Base64.getUrlEncoder()
                .encodeToString(key.getBytes(StandardCharsets.UTF_8));
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("DAO is closed");
        }
    }

    private Path keyPath(String key) {
        return rootDir.resolve(encodeKey(key));
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        ensureOpen();
        validateKey(key);

        Path path = keyPath(key);

        try {
            return Files.readAllBytes(path);
        } catch (NoSuchFileException e) {
            throw new NoSuchElementException("Key not found: " + key, e);
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        ensureOpen();
        validateKey(key);
        validateValue(value);

        Path target = keyPath(key);
        Path temp = rootDir.resolve(encodeKey(key) + ".tmp");

        Files.write(temp, value);

        Files.move(
                temp,
                target,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE
        );
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        ensureOpen();
        validateKey(key);

        Files.deleteIfExists(keyPath(key));
    }

    @Override
    public void close() throws IOException {
        closed.set(true);
    }
}
