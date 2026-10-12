package messageserver.persist;

/**
 * @author ericjbruno
 */
public class MessagePersistanceFactory {

    private MessagePersistance db = null;

    private static MessagePersistanceFactory INSTANCE = new MessagePersistanceFactory();

    public static MessagePersistanceFactory getInstance() {
        return INSTANCE;
    }

    private MessagePersistanceFactory() {
        db = MessageNoSQL.getInstance();
    }

    public MessagePersistance getMessagePersistance() {
        return db;
    }
}
