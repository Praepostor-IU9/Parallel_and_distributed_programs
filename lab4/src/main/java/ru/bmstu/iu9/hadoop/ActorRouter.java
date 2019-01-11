package ru.bmstu.iu9.hadoop;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.routing.*;

public class ActorRouter extends AbstractActor {
    static class OutputRes {
        private String pkg;

        public OutputRes(String pkg) {
            this.pkg = pkg;
        }
    }

    private ActorRef router;
    private ActorRef storage;

    ActorRouter(int n) {
        this.storage = getContext().actorOf(Props.create(ActorResult.class));
        this.router = getContext()
                .actorOf(Props.create(ActorPerform.class, this.storage)
                        .withRouter(new RoundRobinPool(n)));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(App.InputPackage.class, m -> {
                    for (App.InputTest test : m.tests) {
                        this.router.tell(
                                new ActorPerform.InputTestMsg(
                                        m.packageId, test.testName, m.jsScript,
                                        m.functionName, test.expectedResult, test.params.toArray()),
                                getSelf()
                        );
                    }
                })
                .match(OutputRes.class, m -> this.storage.tell(m.pkg, sender()))
//                .match(OutputRes.class, m -> sender().tell("Heh", getSelf()))
                .build();
    }
}
