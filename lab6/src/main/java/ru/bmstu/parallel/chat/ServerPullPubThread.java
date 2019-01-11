package ru.bmstu.parallel.chat;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class ServerPullPubThread implements Runnable {
    private ZMQ.Socket pull;
    private ZMQ.Socket pub;
    private ZContext context;

    ServerPullPubThread(String addrPull) {
        context = new ZContext();
        pull = context.createSocket(ZMQ.PULL);
        pub = context.createSocket(ZMQ.PUB);
        pull.bind(addrPull);
        pub.connect(ServerProxy.XSUB_ADDRESS);
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            String msg = pull.recvStr();
            System.out.println("PullPub Пришло сообщение: "+msg);
            pub.send(msg);
        }
        pub.close();
        pull.close();
        context.destroy();
    }
}
