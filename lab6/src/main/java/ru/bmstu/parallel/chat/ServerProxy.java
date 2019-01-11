package ru.bmstu.parallel.chat;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class ServerProxy {
    static final String XSUB_ADDRESS = "tcp://localhost:17077";
    static final String XPUB_ADDRESS = "tcp://localhost:17078";

    public static void main(String[] args) {
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket xsub = context.socket(ZMQ.XSUB);
        xsub.bind(XSUB_ADDRESS);
        ZMQ.Socket xpub = context.socket(ZMQ.XPUB);
        xpub.bind(XPUB_ADDRESS);
        ZMQ.proxy(xsub, xpub, null);

        xsub.close();
        xpub.close();
        context.term();
    }
}
