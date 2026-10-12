package messageserver.sse;

import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author ericjbruno
 */
public abstract class Destination {

    final protected String name;
    final protected Vector<Object> listeners = new Vector<>();

    public Destination(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public abstract Message getNextMessage() throws InterruptedException;

    public abstract boolean addMessage(String data);
}
