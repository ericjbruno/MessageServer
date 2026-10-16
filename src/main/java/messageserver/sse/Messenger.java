package messageserver.sse;

import io.micronaut.http.HttpResponse;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author ericjbruno
 */
public class Messenger {

    protected final ConcurrentHashMap<String, Destination> destinations
            = new ConcurrentHashMap<>();

    protected boolean sendUpdate(String data, Vector<Object> listeners) {
        if (listeners != null) {
        }

        return false;
    }

    protected boolean sendSSEMessage(PrintWriter out, String data) {
        out.write("event: message\n");
        out.write("data: " + data + "\n\n");
        //out.flush();
        boolean error = out.checkError(); // no need to call flush()
        return !error;
    }

    public Queue getQueue(String name) {
        synchronized (destinations) {
            Queue dest = (Queue) destinations.get(name);
            if (dest == null) {
                dest = new Queue(name);
                destinations.put(name, dest);
            }
            return dest;
        }
    }

    public Topic getTopic(String name) {
        synchronized (destinations) {
            Topic dest = (Topic) destinations.get(name);
            if (dest == null) {
                dest = new Topic(name);
                destinations.put(name, dest);
            }
            return dest;
        }
    }

    public HttpResponse<String> processSend(Destination dest, String data) {
        boolean event = false;
        String id = "";

        try {
            String[] lines = data.split(System.getProperty("line.separator"));
            for (String line : lines) {
                if (line.contains("event:")) {
                    event = true;
                } else if (line.contains("id:")) {
                    id = line;
                } else if (line.contains("data:")) {
                    int start = line.indexOf("data:") + "data:".length();
                    data = line.substring(start).trim();
                    if (!dest.addMessage(data)) {
                        return HttpResponse.serverError("Failed to store message for " + dest.getName());
                    }
                }
            }

            return HttpResponse.ok(dest.getName()).setAttribute("Access-Control-Allow-Origin", "*");
        } catch (Exception e) {
            return HttpResponse.serverError(e.toString());
        }

    }

    protected void removeListener(Object ac) {
        try {
            synchronized (destinations) {
                Iterator<Destination> iter = destinations.values().iterator();
                while (iter.hasNext()) {
                    Destination dest = iter.next();
                    if (dest.listeners.contains(ac)) {
                        dest.listeners.removeElement(ac);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
