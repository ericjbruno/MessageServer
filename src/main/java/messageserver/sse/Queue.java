package messageserver.sse;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import messageserver.persist.MessagePersistance;
import messageserver.persist.MessagePersistanceFactory;

/**
 * @author ericjbruno
 */
public class Queue extends Destination {

    final protected ArrayBlockingQueue<Long> messageIds
            = new ArrayBlockingQueue<>(20000);

    final protected MessagePersistance persistance
            = MessagePersistanceFactory
                    .getInstance()
                    .getMessagePersistance();

    public Queue(String name) {
        super(name);

        // load message IDs
        ArrayList<Long> ids = persistance.getMessageIds(name);
        if (ids != null) {
            messageIds.addAll(ids);
        }

    }

    public Message getNextMessage() throws InterruptedException {
        // Blocking call
        Long messageId = messageIds.take();

        // Load the message data using the ID
        Message message = persistance.getMessage(getName(), messageId);
        return message;
    }

    public boolean addMessage(String message) {
        Long messageId = System.currentTimeMillis();

        try {
            // Persist the message body then save just the message id to the queue
            //
            if (persistance.saveMessage(getName(), messageId, message)) {
                if (messageIds.add(messageId)) {
                    return true;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    public void deleteMessage(Long messageId) {
        persistance.deleteMessage(messageId, name);
    }
}
