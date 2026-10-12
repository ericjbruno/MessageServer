package messageserver;

import io.micronaut.http.HttpParameters;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.sse.Event;
import io.micronaut.session.Session;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import java.util.ArrayList;
import java.util.Optional;
import messageserver.persist.MessagePersistance;
import messageserver.persist.MessagePersistanceFactory;
import messageserver.sse.Message;
import messageserver.sse.Messenger;
import messageserver.sse.Queue;
import org.reactivestreams.Publisher;

/**
 * @author ericjbruno
 */
@Controller("/messageserver/api/")
public class QueueController extends Messenger {

    private MessagePersistance messageDB
            = MessagePersistanceFactory.getInstance().getMessagePersistance();

    public QueueController() {
        log("QueueController");
        loadSavedMessages();
    }

    //
    // Queue Listener calls this. Returns a reactive streams
    // publisher object to emit SSE events
    //
    @Get("/queue/{name}")
    @Produces(MediaType.TEXT_EVENT_STREAM)
    public Publisher<Event<String>> index(Optional<String> name) {
        // Determine queue to listen to
        //
        System.out.println("Queue name: " + name);
        Queue dest = getQueue(name.get());

        return Flowable.<Event<String>, Integer>generate(() -> 0, (i, emitter) -> {
            // Get the message first...
            Message msg = dest.getNextMessage();
            String data = new String(msg.getData());

            // Then deliver it...
            emitter.onNext(
                    Event.of(data)
            );

            // Finally delete it after delivery...
            dest.deleteMessage(msg.getId());
        }).subscribeOn(Schedulers.io());
    }

    //
    // Queue Sender calls this to send messages. They are persisted
    // and then delivered to at most one listener
    //
    @Consumes(MediaType.TEXT_EVENT_STREAM)
    @Post("/queue/publish")
    public HttpResponse queue(Session session,
            HttpRequest<?> request,
            @Body String data) {
        try {
            HttpParameters params = request.getParameters();
            String queueName = params.getFirst("name").orElse(null);
            Queue dest = getQueue(queueName);

            return processSend(dest, data);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return HttpResponse.status(HttpStatus.UNAUTHORIZED, "Not authenticated");
    }

    private void loadSavedMessages() {
        ArrayList<String> queueNames = messageDB.getStoredDestinations();
        for (String queueName : queueNames) {
            // Get the queue (loads all queued messages as side effect)
            Queue dest = getQueue(queueName);
        }
    }

    public void log(String s) {
        System.out.println("[QueueController] " + s);
    }

}
