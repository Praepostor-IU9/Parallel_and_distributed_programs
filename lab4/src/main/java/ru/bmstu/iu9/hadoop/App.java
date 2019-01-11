package ru.bmstu.iu9.hadoop;

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import akka.pattern.Patterns;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.util.Timeout;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;


public class App extends AllDirectives {
    public static void main(String[] args) throws IOException {
        ActorSystem system = ActorSystem.create("lab4");
        final Http http = Http.get(system);
        final ActorMaterializer materializer = ActorMaterializer.create(system);

        App app = new App();
        ActorRef router = system.actorOf(Props.create(ActorRouter.class, 5));

        final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow = app.createRoute(system, router).flow(system, materializer);
        final CompletionStage<ServerBinding> binding = http.bindAndHandle(
                routeFlow,
                ConnectHttp.toHost("localhost", 17077),
                materializer
        );
        System.out.println("Server online at http://localhost:17077/\nPress RETURN to stop...");
        System.in.read();
        binding
                .thenCompose(ServerBinding::unbind)
                .thenAccept(undound -> system.terminate());
    }

    private Route createRoute(ActorSystem system, ActorRef router) {
        return route(
                path("result", () ->
                {
                    return route(
                            get(() ->
                            {
                                return parameter("PackageId", pkg -> {
                                    final Timeout timeout = Timeout.durationToTimeout(
                                            FiniteDuration.apply(5, TimeUnit.SECONDS)
                                    );
                                    Future<Result> res = Patterns.ask(
                                            router,
                                            new ActorRouter.OutputRes(pkg),
                                            timeout
                                    ).map(r -> (Result) r, system.dispatcher());
                                    return completeOKWithFuture(res, Jackson.marshaller());
                                });
                            })
                    );
                }),
                path("test", () ->
                        route(
                                post(() ->
                                        entity(Jackson.unmarshaller(InputPackage.class), msg -> {
                                            router.tell(msg, ActorRef.noSender());
                                            return complete("Test started\n");
                                        }))
                        ))
        );
    }

    static class InputTest {
        final String testName;
        final String expectedResult;
        final List<Object> params;

        @JsonCreator
        InputTest(@JsonProperty("testName") String testName,
                  @JsonProperty("expectedResult") String expectedResult,
                  @JsonProperty("params") List<Object> params) {
            this.testName = testName;
            this.expectedResult = expectedResult;
            this.params = params;
        }
    }

    static class InputPackage {
        final String packageId;
        final String jsScript;
        final String functionName;
        final List<InputTest> tests;

        @JsonCreator
        InputPackage(@JsonProperty("packageId") String packageId,
                     @JsonProperty("jsScript") String jsScript,
                     @JsonProperty("functionName") String functionName,
                     @JsonProperty("tests") List<InputTest> tests) {
            this.packageId = packageId;
            this.jsScript = jsScript;
            this.functionName = functionName;
            this.tests = tests;
        }
    }
}
