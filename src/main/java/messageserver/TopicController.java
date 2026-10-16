package messageserver;

import messageserver.sse.Messenger;
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
import java.util.Optional;
import messageserver.sse.Topic;
import org.reactivestreams.Publisher;

/**
 * @author ericjbruno
 */
@Controller("/messageserver/api")
public class TopicController extends Messenger {
    //
    // Pub/Sub
    //

    // Topic Subscriptions are handled here
    //
    @Get("/topic/{name}")
    @Produces(MediaType.TEXT_EVENT_STREAM)
    public Publisher<Event<String>> index(Optional<String> name) {
        // Determine topic to listen to
        //
        System.out.println("Topic name: " + name);
        Topic dest = getTopic(name.get());

        return Flowable.<Event<String>, Integer>generate(() -> 0, (i, emitter) -> {
            String data = new String(dest.getNextMessage().getData());

            emitter.onNext(
                    Event.of(new String(data))
            );
        }).subscribeOn(Schedulers.io());
    }

    // Topic publishers and publications are handled here
    //
    @Consumes(MediaType.TEXT_EVENT_STREAM)
    @Post("/topic/publish")
    public HttpResponse<?> topic(Session session,
            HttpRequest<?> request,
            @Body String data) {
        try {
            HttpParameters params = request.getParameters();
            String topicName = params.getFirst("name").orElse(null);
            Topic dest = getTopic(topicName);

            return processSend(dest, data);
        } catch (Exception e) {
            //e.printStackTrace();
        }

        return HttpResponse.status(HttpStatus.UNAUTHORIZED, "Not authenticated");
    }
}
