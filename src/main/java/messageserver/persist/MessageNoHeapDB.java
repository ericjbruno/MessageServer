package messageserver.persist;

import com.alluretechnology.noheapdb.DataStore;
import com.alluretechnology.noheapdb.NoHeapDB;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import messageserver.sse.Message;

/**
 * Message persistence using NoHeapDB. Message bodies are kept off-heap in a
 * single NoHeapDB store; only message IDs and store keys are kept on-heap.
 *
 * NoHeapDB's PERSISTED storage isn't implemented yet, so this uses an
 * IN_MEMORY store and messages do not survive a server restart.
 *
 * @author ericjbruno
 */
public class MessageNoHeapDB implements MessagePersistance {

    private static final String STORE_NAME = "messages";

    // NoHeapDB allocates at least 1GB of direct memory per in-memory store,
    // which is why all destinations share one store. Its buffers hold just
    // under 1024MB, so asking for 1024 would allocate a second 1GB buffer.
    // The journal expands on its own if it fills up
    private static final long STORE_SIZE_MB = 1023;

    private final NoHeapDB db = new NoHeapDB();

    // NoHeapDB can't list the keys in a store, so track each destination's
    // message IDs, and the store key used for each, here
    private final ConcurrentHashMap<String, ConcurrentSkipListMap<Long, String>> destinations
            = new ConcurrentHashMap<>();

    // NoHeapDB's index identifies a key by its hashCode and length only, so a
    // second key matching both silently overwrites the first. Track the
    // signatures of live keys so a new key never matches one of them
    private final Set<Long> keySignatures = ConcurrentHashMap.newKeySet();
    private final Object keyLock = new Object();

    private static MessageNoHeapDB INSTANCE = new MessageNoHeapDB();

    public static MessageNoHeapDB getInstance() {
        return INSTANCE;
    }

    private MessageNoHeapDB() {
        try {
            db.createStore(STORE_NAME, DataStore.Storage.IN_MEMORY, STORE_SIZE_MB);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create NoHeapDB store", e);
        }
    }

    public boolean saveMessage(String destinationName,
            Long messageId,
            String message) throws Exception {
        String key = allocateKey(destinationName, messageId);
        if (!db.putString(STORE_NAME, key, message)) {
            releaseKey(key);
            return false;
        }

        String previousKey = destinations
                .computeIfAbsent(destinationName, d -> new ConcurrentSkipListMap<>())
                .put(messageId, key);
        if (previousKey != null) {
            // Same message ID saved again; drop the older copy
            db.remove(STORE_NAME, previousKey);
            releaseKey(previousKey);
        }
        return true;
    }

    public Message getMessage(String destinationName, Long messageId) {
        String key = getKey(destinationName, messageId);
        String data = (key == null) ? null : db.getString(STORE_NAME, key);
        if (data == null) {
            log("Message " + destinationName + "/" + messageId + " is null!");
            return null;
        }

        return new Message(messageId, data);
    }

    public String getNextMessage(String destinationName) {
        return null;
    }

    public boolean deleteMessage(Long messageId, String destinationName) {
        Map<Long, String> ids = destinations.get(destinationName);
        String key = (ids == null) ? null : ids.remove(messageId);
        if (key == null) {
            return false;
        }

        boolean deleted = db.remove(STORE_NAME, key);
        releaseKey(key);
        return deleted;
    }

    @Override
    public ArrayList<String> getStoredDestinations() {
        ArrayList<String> names = new ArrayList<>();
        for (Map.Entry<String, ConcurrentSkipListMap<Long, String>> entry : destinations.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                names.add(entry.getKey());
            }
        }
        return names;
    }

    @Override
    public ArrayList<Long> getMessageIds(String destinationName) {
        Map<Long, String> ids = destinations.get(destinationName);
        if (ids == null) {
            return new ArrayList<>();
        }

        // Sorted ascending, matching the order messages were stored in
        return new ArrayList<>(ids.keySet());
    }

    private String getKey(String destinationName, Long messageId) {
        Map<Long, String> ids = destinations.get(destinationName);
        return (ids == null) ? null : ids.get(messageId);
    }

    private String allocateKey(String destinationName, Long messageId) {
        String baseKey = destinationName + "/" + messageId;
        String key = baseKey;
        int salt = 0;
        synchronized (keyLock) {
            // Add a suffix until the key is safe to store and unique in the index
            while (!isStorableLength(key) || !keySignatures.add(signature(key))) {
                key = baseKey + "#" + (++salt);
            }
        }
        return key;
    }

    private void releaseKey(String key) {
        keySignatures.remove(signature(key));
    }

    private static long signature(String key) {
        // The index stores the key length as a single byte
        return ((long) key.hashCode() << 32) | (key.length() & 0xFF);
    }

    private static boolean isStorableLength(String key) {
        // The index's length byte doubles as its slot marker, where 0 means
        // empty and 2 means deleted, so keys can't have either length
        int lengthByte = key.length() & 0xFF;
        return lengthByte != 0 && lengthByte != 2;
    }

    public static void log(String s) {
        System.out.println("[MessageNoHeapDB] " + s);
    }

}
