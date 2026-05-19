package company.vk.edu.distrib.compute.dariaprindina;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.AuditableKVService;
import company.vk.edu.distrib.compute.Dao;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@SuppressWarnings({
    "PMD.AvoidUsingVolatile",
    "PMD.AvoidSynchronizedStatement"
})
public class DPKvService implements AuditableKVService {
    private static final Logger log = LoggerFactory.getLogger(DPKvService.class);
    private static final String ID_PARAM_PREFIX = "id=";
    private static final String TOPIC_AUDIT = "audit";

    private final HttpServer server;
    private final Dao<byte[]> dao;
    private volatile String bootstrapServers;
    private volatile boolean asyncEnabled;
    private volatile Producer<String, String> producer;

    public DPKvService(int port, Dao<byte[]> dao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;
        initServer();
    }

    private void initServer() {
        server.createContext("/v0/status", new ErrorHttpHandler(httpExchange -> {
            final var method = httpExchange.getRequestMethod();
            if (Objects.equals("GET", method)) {
                sendResponse(httpExchange, 200, null);
            } else {
                sendResponse(httpExchange, 405, null);
            }
        }));

        server.createContext("/v0/entity", new ErrorHttpHandler(httpExchange -> {
            final var method = httpExchange.getRequestMethod();
            final var query = httpExchange.getRequestURI().getQuery();
            final var id = parseId(query);
            final long timestamp = System.currentTimeMillis();
            sendAudit(method, id, timestamp);
            if ("GET".equals(method)) {
                final var value = dao.get(id);
                sendResponse(httpExchange, 200, value);
            } else if ("PUT".equals(method)) {
                try (var requestBody = httpExchange.getRequestBody()) {
                    dao.upsert(id, requestBody.readAllBytes());
                }
                sendResponse(httpExchange, 201, null);
            } else if ("DELETE".equals(method)) {
                dao.delete(id);
                sendResponse(httpExchange, 202, null);
            } else {
                sendResponse(httpExchange, 405, null);
            }
        }));
    }

    @Override
    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        resetProducer();
    }

    @Override
    public void setAsync(boolean enabled) {
        this.asyncEnabled = enabled;
    }

    private static String parseId(String query) {
        if (query != null && query.startsWith(ID_PARAM_PREFIX) && query.length() > ID_PARAM_PREFIX.length()) {
            return query.substring(ID_PARAM_PREFIX.length());
        }
        throw new IllegalArgumentException("bad query");
    }

    @Override
    public void start() {
        log.info("Starting");
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
        closeProducer();
        try {
            dao.close();
        } catch (IOException e) {
            log.error("Failed to close dao", e);
        }
        log.info("Stopped");
    }

    private static void sendResponse(HttpExchange exchange, int code, byte[] body) throws IOException {
        final byte[] responseBody = body == null ? new byte[0] : body;
        exchange.sendResponseHeaders(code, responseBody.length);
        if (responseBody.length > 0) {
            try (OutputStream outputStream = exchange.getResponseBody()) {
                outputStream.write(responseBody);
            }
            return;
        }
        exchange.getResponseBody().close();
    }

    private static final class ErrorHttpHandler implements HttpHandler {
        private final HttpHandler delegate;

        private ErrorHttpHandler(HttpHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                delegate.handle(exchange);
            } catch (IllegalArgumentException e) {
                sendResponse(exchange, 400, null);
            } catch (NoSuchElementException e) {
                sendResponse(exchange, 404, null);
            } catch (IOException e) {
                sendResponse(exchange, 500, null);
            }
        }
    }

    private void sendAudit(String method, String id, long timestamp) {
        final Producer<String, String> localProducer = ensureProducer();
        if (localProducer == null) {
            return;
        }
        final String payload = DPKvAuditUtils.serialize(method, id, timestamp);
        final ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_AUDIT, id, payload);
        if (asyncEnabled) {
            localProducer.send(record);
            return;
        }
        try {
            localProducer.send(record).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while sending audit event", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Failed to send audit event", e);
        }
    }

    private Producer<String, String> ensureProducer() {
        Producer<String, String> localProducer = producer;
        if (localProducer != null) {
            return localProducer;
        }
        final String servers = bootstrapServers;
        if (servers == null || servers.isBlank()) {
            return null;
        }
        synchronized (this) {
            if (producer != null) {
                return producer;
            }
            final Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producer = new KafkaProducer<>(properties);
            return producer;
        }
    }

    private void resetProducer() {
        synchronized (this) {
            if (producer != null) {
                producer.close();
                producer = null;
            }
        }
    }

    private void closeProducer() {
        synchronized (this) {
            if (producer == null) {
                return;
            }
            producer.close();
            producer = null;
        }
    }
}
