package messageserver.persist;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import messageserver.sse.Message;
import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.ValueVersion;

/**
 * @author ericjbruno
 */
public class MessageNoSQL implements MessagePersistance {

    private final int NUM_ROWS_IN_MESSAGE_TABLE = 1000;

    // message table 
    private String storeName = "kvstore";
    private String host = "127.0.0.1";
    private String port = "5001";
    private KVStore store = null;
    private int num = 0;

    private static MessageNoSQL INSTANCE = new MessageNoSQL();

    public static MessageNoSQL getInstance() {
        return INSTANCE;
    }

    ///////////////////////////////////////////////////////////////////////////
    // For testing 
    public static void main(String[] args) {
        MessageNoSQL nosql = MessageNoSQL.getInstance();
    }

    private MessageNoSQL() {
//        KVStoreConfig config = new KVStoreConfig(storeName, host + ":" + port);
//        store = KVStoreFactory.getStore(config);
        KVStoreConfig config = new KVStoreConfig(storeName, host + ":" + port);
        store = KVStoreFactory.getStore(config);
    }

    public boolean saveMessage(String destinationName,
            Long messageId,
            String message) throws Exception {
        String idStr = messageId.toString();
        store.put(Key.createKey(destinationName, idStr),
                Value.createValue(message.getBytes()));
        return true;
    }

    public Message getMessage(String destinationName, Long messageId) {
        String idStr = messageId.toString();
        Key key = Key.createKey(destinationName, idStr);
        ValueVersion value = store.get(key);
        if (value == null || value.getValue() == null) {
            log("Message " + key + " is null!");
            return null;
        }

        Value val = value.getValue();
        String data = new String(val.getValue());
        return new Message(messageId, data);
    }

    public String getNextMessage(String destinationName) {
        return null;
    }

    public boolean deleteMessage(Long messageId, String destinationName) {
        String idStr = "" + messageId;
        Key key = Key.createKey(destinationName, idStr);
        boolean deleted = store.delete(key);
        return deleted;
    }

    @Override
    public ArrayList<String> getStoredDestinations() {
        ArrayList<String> keys = new ArrayList<>();
        Iterator<Key> iter
                = store.storeKeysIterator(
                        Direction.UNORDERED, 1000);

        while (iter.hasNext()) {
            Key k = iter.next();
            String queueName = k.getMajorPath().get(0);
            if (!keys.contains(queueName)) {
                keys.add(queueName);
                System.out.println(queueName);
            }
        }

        return keys;
    }

    @Override
    public ArrayList<Long> getMessageIds(String destinationName) {
        ArrayList<Long> messageIds = new ArrayList<>();

        // Get all records with the major key that matches the dest name
        //
        Key key = Key.createKey(destinationName);
        SortedMap<Key, ValueVersion> myRecords = null;
        try {
            myRecords = store.multiGet(key, null, null);
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (Map.Entry<Key, ValueVersion> entry : myRecords.entrySet()) {
            String destName = entry.getKey().getMajorPath().get(0);
            assert (destName.equals(destinationName));
            String messageId = entry.getKey().getMinorPath().get(0);
            messageIds.add(Long.valueOf(messageId));
        }

        return messageIds;
    }

    public static void log(String s) {
        System.out.println("[MessageNoSQL] " + s);
    }

}
