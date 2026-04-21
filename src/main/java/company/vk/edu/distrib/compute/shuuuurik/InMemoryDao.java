package company.vk.edu.distrib.compute.shuuuurik;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory implementation of Dao.
 */
public class InMemoryDao implements Dao<byte[]> {

    private final ConcurrentMap<String, byte[]> storage = new ConcurrentHashMap<>();

    private static void validateKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key must not be null or blank");
        }
    }

    private static void validateValue(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("Value must not be null");
        }
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        validateKey(key);

        byte[] value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("No value for key: " + key);
        }

        return Arrays.copyOf(value, value.length);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        validateKey(key);
        validateValue(value);

        storage.put(key, Arrays.copyOf(value, value.length));
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        validateKey(key);

        storage.remove(key);
    }

    @Override
    public void close() throws IOException {
        storage.clear();
    }
}
