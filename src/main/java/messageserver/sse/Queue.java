package messageserver.sse;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import messageserver.persist.MessagePersistance;
import messageserver.persist.MessagePersistanceFactory;

/**
 * @author ericjbruno
 */
public class Queue extends Destination {

    // Unbounded, since every persisted message must have its ID queued for
    // delivery. A fixed capacity rejected IDs once full (stranding messages
    // that were already persisted) and failed to reload a large backlog
    final protected LinkedBlockingQueue<Long> messageIds
            = new LinkedBlockingQueue<>();

    final protected MessagePersistance persistance
            = MessagePersistanceFactory
                    .getInstance()
                    .getMessagePersistance();

    // Last message ID issued for this queue. IDs must be unique per queue
    // since they're the persistence key, so never reuse one even when
    // several messages arrive within the same millisecond
    final protected AtomicLong lastMessageId = new AtomicLong(0);

    public Queue(String name) {
        super(name);

        // load message IDs
        ArrayList<Long> ids = persistance.getMessageIds(name);
        if (ids != null) {
            messageIds.addAll(ids);
            for (Long id : ids) {
                lastMessageId.accumulateAndGet(id, Math::max);
            }
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
        Long messageId = nextMessageId();

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

    protected long nextMessageId() {
        // Use the current time, or last ID + 1 if the clock hasn't advanced
        return lastMessageId.updateAndGet(
                last -> Math.max(last + 1, System.currentTimeMillis()));
    }

    public void deleteMessage(Long messageId) {
        persistance.deleteMessage(messageId, name);
    }
}
