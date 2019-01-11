package ru.bmstu.parallel.chat;

import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import io.undertow.server.handlers.resource.ResourceHandler;
import io.undertow.util.Headers;
import io.undertow.websockets.WebSocketProtocolHandshakeHandler;
import io.undertow.websockets.core.AbstractReceiveListener;
import io.undertow.websockets.core.BufferedTextMessage;
import io.undertow.websockets.core.WebSocketChannel;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.Scanner;

public class ChatApp {

    private static String addrPull;

    private static final ThreadLocal<ZMQ.Socket> push = new ThreadLocal<ZMQ.Socket>() {
        @Override
        protected ZMQ.Socket initialValue() {
            ZContext context = new ZContext();
            ZMQ.Socket push = context.createSocket(ZMQ.PUSH);
            push.connect(addrPull);
            return push;
        }
    };

    public static void main(final String[] args) {
        Scanner input = new Scanner(System.in);

        System.out.print("Введите порт для PULL Socket\ntcp://localhost:");
        ChatApp.addrPull = "tcp://localhost:"+input.next();

        System.out.print("Введите порт для Server\nlocalhost:");
        int port = input.nextInt();
        System.out.println(port);
        input.close();

        WebSocketProtocolHandshakeHandler webSocketHandler = Handlers.websocket(
                (exchange, channel) -> {
                    channel.getReceiveSetter().set(
                            new AbstractReceiveListener() {
                                @Override
                                protected void onFullTextMessage(WebSocketChannel channel, BufferedTextMessage message) {
                                    push.get().send(message.getData());
                                }
                            });
                    channel.resumeReceives();
                });

//        PULL & PUB
        Thread pullPub = new Thread(new ServerPullPubThread(addrPull));
//        SUB
        Thread sub = new Thread(new ServerSubThread(webSocketHandler));
//        Start
        pullPub.start();
        sub.start();

        ResourceHandler resourceHandler = Handlers.resource(new ClassPathResourceManager(HttpServer.class.getClassLoader(), ""))
                .addWelcomeFiles("index"+port+".html");

        Undertow server = Undertow.builder()
                .addHttpListener(port, "localhost")
                .setHandler(Handlers.path()
                        .addPrefixPath("/chatsocket", webSocketHandler)
                        .addPrefixPath("/", resourceHandler)
                        .addPrefixPath("/connections", Handlers.path(exchange -> {
                            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                            exchange.getResponseSender().send("" + webSocketHandler.getPeerConnections().size());
                        }))
                )
                .build();
        server.start();
    }
}
