package ru.bmstu.parallel.chat;

import io.undertow.websockets.WebSocketProtocolHandshakeHandler;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class ServerSubThread implements Runnable {
    private ZContext context;
    private ZMQ.Socket sub;
    private WebSocketProtocolHandshakeHandler webSocketHandler;

    ServerSubThread(WebSocketProtocolHandshakeHandler webSocketHandler) {
		this.webSocketHandler = webSocketHandler;
        context = new ZContext();
        sub = context.createSocket(ZMQ.SUB);
        sub.connect(ServerProxy.XPUB_ADDRESS);
        sub.subscribe("");
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            String msg = sub.recvStr(0);
            System.out.println("Sub Пришло сообщение: "+msg);
            for (WebSocketChannel session : webSocketHandler.getPeerConnections()) {
                WebSockets.sendText(msg, session, null);
            }
        }
        sub.close();
        context.destroy();
    }
}
