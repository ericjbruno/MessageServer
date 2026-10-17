package messageserver.persist;

/**
 * @author ericjbruno
 */
public class MessagePersistanceFactory {

    // Selects the message store: "noheapdb" (the default) or "oraclekv".
    // Set with -Dmessageserver.persistence=... or the
    // MESSAGESERVER_PERSISTENCE environment variable
    public static final String PERSISTENCE_PROPERTY = "messageserver.persistence";
    public static final String PERSISTENCE_ENV = "MESSAGESERVER_PERSISTENCE";

    private MessagePersistance db = null;

    private static MessagePersistanceFactory INSTANCE = new MessagePersistanceFactory();

    public static MessagePersistanceFactory getInstance() {
        return INSTANCE;
    }

    private MessagePersistanceFactory() {
        String type = System.getProperty(PERSISTENCE_PROPERTY);
        if (type == null) {
            type = System.getenv(PERSISTENCE_ENV);
        }
        if (type == null || type.isBlank()) {
            type = "noheapdb";
        }

        // Only the selected implementation's class is loaded, so the other
        // database never needs to be running
        switch (type.trim().toLowerCase()) {
            case "oraclekv":
                db = MessageNoSQL.getInstance();
                break;
            case "noheapdb":
                db = MessageNoHeapDB.getInstance();
                break;
            default:
                throw new IllegalArgumentException("Unknown " + PERSISTENCE_PROPERTY
                        + " '" + type + "'; expected 'oraclekv' or 'noheapdb'");
        }

        log("Using message persistence: " + type);
    }

    public MessagePersistance getMessagePersistance() {
        return db;
    }

    public static void log(String s) {
        System.out.println("[MessagePersistanceFactory] " + s);
    }
}
