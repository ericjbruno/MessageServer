package messageserver.persist;

import com.alluretechnology.noheapdb.DataStore;
import com.alluretechnology.noheapdb.NoHeapDB;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import messageserver.sse.Message;

/**
 * Message persistence using NoHeapDB. Message bodies are kept off-heap in a
 * single NoHeapDB store, keyed by "destination/messageId"; only the message
 * IDs are kept on-heap.
 *
 * The store is IN_MEMORY by default, so messages don't survive a restart.
 * Set -Dmessageserver.noheapdb.storage=persisted (or the
 * MESSAGESERVER_NOHEAPDB_STORAGE environment variable) to keep them in files
 * under ~/JavaOffHeap, where they're reloaded when the server restarts.
 *
 * @author ericjbruno
 */
public class MessageNoHeapDB implements MessagePersistance {

    public static final String STORAGE_PROPERTY = "messageserver.noheapdb.storage";
    public static final String STORAGE_ENV = "MESSAGESERVER_NOHEAPDB_STORAGE";

    private static final String STORE_NAME = "messages";

    // Initial size; the journal adds chunks on its own as it fills up
    private static final long STORE_SIZE_MB = 256;

    private final NoHeapDB db = new NoHeapDB();

    // Each destination's message IDs, sorted, so a destination's messages can
    // be listed without scanning every key in the store
    private final ConcurrentHashMap<String, ConcurrentSkipListSet<Long>> destinations
            = new ConcurrentHashMap<>();

    private static MessageNoHeapDB INSTANCE = new MessageNoHeapDB();

    public static MessageNoHeapDB getInstance() {
        return INSTANCE;
    }

    private MessageNoHeapDB() {
        DataStore.Storage storage = getStorageType();
        try {
            // Reopens the existing files if the store was persisted before
            db.createStore(STORE_NAME, storage, STORE_SIZE_MB);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create NoHeapDB store", e);
        }

        if (storage == DataStore.Storage.PERSISTED) {
            loadMessageIds();

            // Close cleanly on shutdown so the next start can reuse the store's
            // index instead of rebuilding it from the journal
            Runtime.getRuntime().addShutdownHook(new Thread(() -> db.closeStore(STORE_NAME)));
        }

        log("Using " + storage + " store");
    }

    private static DataStore.Storage getStorageType() {
        String type = System.getProperty(STORAGE_PROPERTY);
        if (type == null) {
            type = System.getenv(STORAGE_ENV);
        }
        if (type == null || type.isBlank()) {
            return DataStore.Storage.IN_MEMORY;
        }

        switch (type.trim().toLowerCase()) {
            case "in_memory":
                return DataStore.Storage.IN_MEMORY;
            case "persisted":
                return DataStore.Storage.PERSISTED;
            default:
                throw new IllegalArgumentException("Unknown " + STORAGE_PROPERTY
                        + " '" + type + "'; expected 'in_memory' or 'persisted'");
        }
    }

    private void loadMessageIds() {
        int count = 0;
        Iterator<String> keys = db.keys(STORE_NAME);
        while (keys.hasNext()) {
            String key = keys.next();
            int slash = key.lastIndexOf('/');
            try {
                Long messageId = Long.valueOf(key.substring(slash + 1));
                getIds(key.substring(0, slash)).add(messageId);
                count++;
            } catch (RuntimeException e) {
                log("Skipping unrecognized key '" + key + "'");
            }
        }
        log("Loaded " + count + " stored messages");
    }

    public boolean saveMessage(String destinationName,
            Long messageId,
            String message) throws Exception {
        if (!db.putString(STORE_NAME, toKey(destinationName, messageId), message)) {
            return false;
        }

        getIds(destinationName).add(messageId);
        return true;
    }

    public Message getMessage(String destinationName, Long messageId) {
        String data = db.getString(STORE_NAME, toKey(destinationName, messageId));
        if (data == null) {
            log("Message " + toKey(destinationName, messageId) + " is null!");
            return null;
        }

        return new Message(messageId, data);
    }

    public String getNextMessage(String destinationName) {
        return null;
    }

    public boolean deleteMessage(Long messageId, String destinationName) {
        Set<Long> ids = destinations.get(destinationName);
        if (ids == null || !ids.remove(messageId)) {
            return false;
        }

        return db.remove(STORE_NAME, toKey(destinationName, messageId));
    }

    @Override
    public ArrayList<String> getStoredDestinations() {
        ArrayList<String> names = new ArrayList<>();
        for (Map.Entry<String, ConcurrentSkipListSet<Long>> entry : destinations.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                names.add(entry.getKey());
            }
        }
        return names;
    }

    @Override
    public ArrayList<Long> getMessageIds(String destinationName) {
        Set<Long> ids = destinations.get(destinationName);
        if (ids == null) {
            return new ArrayList<>();
        }

        // Sorted ascending, matching the order messages were stored in
        return new ArrayList<>(ids);
    }

    private ConcurrentSkipListSet<Long> getIds(String destinationName) {
        return destinations.computeIfAbsent(destinationName, d -> new ConcurrentSkipListSet<>());
    }

    private static String toKey(String destinationName, Long messageId) {
        return destinationName + "/" + messageId;
    }

    public static void log(String s) {
        System.out.println("[MessageNoHeapDB] " + s);
    }

}
