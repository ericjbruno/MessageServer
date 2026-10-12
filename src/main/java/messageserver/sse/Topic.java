package messageserver.sse;

/**
 * @author ericjbruno
 */
public class Topic extends Destination {

    protected Message lastMessage = null;
    final protected Object topicMonitor = new Object();

    public Topic(String name) {
        super(name);
    }

    public Message getNextMessage() throws InterruptedException {
        synchronized (topicMonitor) {
            topicMonitor.wait();
        }

        return lastMessage;
    }

    public boolean addMessage(String msgData) {
        Long messageId = System.currentTimeMillis();
        Message msg = new Message(messageId, msgData);
        lastMessage = msg;

        // Notify ALL listeners of the message
        synchronized (topicMonitor) {
            topicMonitor.notifyAll();
        }

        return true;
    }
}
