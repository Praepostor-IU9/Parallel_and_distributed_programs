package ru.bmstu.iu9.hadoop;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class ActorPerform extends AbstractActor {
    static class InputTestMsg {
        private String pkg;
        private String name;
        private String script;
        private String functionName;
        private String res;
        private Object[] args;

        InputTestMsg(String pkg, String name, String script, String functionName, String res, Object[] args) {
            this.pkg = pkg;
            this.name = name;
            this.script = script;
            this.functionName = functionName;
            this.res = res;
            this.args = args;
        }
    }

    private static String checkTest(String script, String functionName, Object... args) throws ScriptException, NoSuchMethodException {
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
        engine.eval(script);
        Invocable invocable = (Invocable) engine;
        return invocable.invokeFunction(functionName, args).toString();
    }

    private final ActorRef storage;

    public ActorPerform(ActorRef storage) {
        this.storage = storage;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(InputTestMsg.class, m -> {
                    boolean res = false;
                    String descr;
                    try {
                        String out = ActorPerform.checkTest(m.script, m.functionName, m.args);
                        res = out.equals(m.res);
                        descr = (res)? "OK": "Expected: " + m.res + " Output: " + out;
                    } catch (ScriptException e) {
                        descr = "Error: ScriptException\n" + e.getMessage();
                    } catch (NoSuchMethodException e) {
                        descr = "Error: NoSuchMethodException\n" + e.getMessage();
                    }
                    this.storage.tell(new ActorResult.InputResMsg(m.pkg, new Test(m.name, res, descr)), getSelf());
                }).build();
    }
}
