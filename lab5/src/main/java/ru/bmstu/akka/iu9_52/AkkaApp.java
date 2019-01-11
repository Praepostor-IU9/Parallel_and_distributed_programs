package ru.bmstu.akka.iu9_52;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpHeader;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.ResponseEntity;
import akka.http.javadsl.model.headers.RawHeader;
import akka.http.javadsl.server.AllDirectives;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Graph;
import akka.stream.javadsl.*;
import scala.util.Try;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.CompletionStage;

public class AkkaApp {

    public static void main(String[] args) throws IOException {
        System.out.println("Start!");
        ActorSystem system = ActorSystem.create("lab5");
        final Http http = Http.get(system);
        final ActorMaterializer materializer = ActorMaterializer.create(system);

        AkkaApp app = new AkkaApp();

        final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow = createFlow(http, system, materializer);

        final CompletionStage<ServerBinding> binding = http.bindAndHandle(
                routeFlow,
                ConnectHttp.toHost("localhost", 17077),
                materializer
        );

        Flow.<Pair<HttpRequest, Integer>>create();
        System.out.println("Server online at http://localhost:17077/\nPress RETURN to stop...");
        System.in.read();
        binding
                .thenCompose(ServerBinding::unbind)
                .thenAccept(undound -> system.terminate());
    }

    private static Flow<HttpRequest, HttpResponse, NotUsed>
    createFlow(Http http,
               ActorSystem system,
               ActorMaterializer materializer) {

        return Flow.of(HttpRequest.class)
                .map(req -> {
                    Map<String, String> paramsMap = req.getUri().query().toMap();
                    if (!paramsMap.containsKey("testUrl") || !paramsMap.containsKey("count")) {
                        System.out.println(paramsMap.toString());
                        return new Pair<HttpRequest, Integer>(HttpRequest.create("localhost"), 0);
                    }
                    String url = paramsMap.get("testUrl");
                    Integer count = Integer.parseInt(paramsMap.get("count"));
                    return new Pair<HttpRequest, Integer>(HttpRequest.create(url), count);
                })
                .mapAsync(4, r -> {
                    Flow<Pair<HttpRequest, Long>, Pair<Try<HttpResponse>, Long>, NotUsed> httpClient = http.superPool(materializer);
                    Sink<Pair<Try<HttpResponse>, Long>, CompletionStage<Long>> fold = Sink.fold(0L, (agg, next) -> agg + System.currentTimeMillis() - next.second());
                    Sink<Pair<HttpRequest, Integer>, CompletionStage<Long>> testSink = Flow.<Pair<HttpRequest, Integer>>create()
                            .mapConcat(pair -> new ArrayList<Pair<HttpRequest, Integer>>(Collections.nCopies(pair.second(), pair)))
                            .map(pair -> new Pair<HttpRequest, Long>(pair.first(), System.currentTimeMillis()))
                            .via(httpClient)
                            .toMat(fold, Keep.right());
                    return Source.from(Collections.singletonList(r))
                            .toMat(testSink, Keep.right()).run(materializer)
                            .thenApply(sum -> sum/(float)((r.second() == 0)? 1:r.second()));
                })
                .map(res -> {
                    System.out.println(res);
                    String mean = new DecimalFormat("#0.00").format(res);
                    return HttpResponse.create()
                            .withStatus(200)
                            .withEntity("Среднее время отклика: "+mean+" ms");
                });
    }
}
