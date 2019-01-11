package ru.bmstu.iu9.hadoop;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ActorResult extends AbstractActor {
    static class InputResMsg {
        private String pkg;
        private Test test;

        InputResMsg(String pkg, Test test) {
            this.pkg = pkg;
            this.test = test;
        }
    }

    private Map<String, ArrayList<Test>> storage = new HashMap<String, ArrayList<Test>>();
    private ActorRef source;

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(InputResMsg.class, m -> {
                    if (storage.containsKey(m.pkg)) {
                        storage.get(m.pkg).add(m.test);
                    } else {
                        ArrayList<Test> tests = new ArrayList<Test>();
                        tests.add(m.test);
                        storage.put(m.pkg, tests);
                    }
                })
                .match(String.class, m -> {
                    ArrayList<Test> list = storage.getOrDefault(m, new ArrayList<Test>());
                    Test[] arr = new Test[list.size()];
                    arr = list.toArray(arr);
                    sender().tell(
                            new Result(arr),
                            getSelf()
                    );
                }).build();
    }
}
