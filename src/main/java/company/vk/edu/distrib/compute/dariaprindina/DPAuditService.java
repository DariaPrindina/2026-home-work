package company.vk.edu.distrib.compute.dariaprindina;

import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings({
    "PMD.AvoidUsingVolatile",
    "PMD.AvoidSynchronizedStatement"
})
public class DPAuditService implements AuditService {
    private static final String TOPIC_AUDIT = "audit";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(200);

    private final String bootstrapServers;
    private final String consumerGroupId;
    private final AtomicBoolean running;
    private final List<AuditEvent> entries;
    private final Object lifecycleLock;

    private volatile KafkaConsumer<String, String> consumer;
    private volatile Thread thread;

    public DPAuditService(String bootstrapServers, String consumerGroupId) {
        this.bootstrapServers = bootstrapServers;
        this.consumerGroupId = consumerGroupId;
        this.running = new AtomicBoolean(false);
        this.entries = new CopyOnWriteArrayList<>();
        this.lifecycleLock = new Object();
    }

    @Override
    public void start() {
        synchronized (lifecycleLock) {
            if (running.get()) {
                return;
            }
            running.set(true);
            consumer = createConsumer();
            consumer.subscribe(List.of(TOPIC_AUDIT));
            thread = new Thread(this::pollLoop, "daria-audit-consumer-" + consumerGroupId);
            thread.start();
        }
    }

    @Override
    public void stop() {
        synchronized (lifecycleLock) {
            running.set(false);
            final Thread localThread = thread;
            if (localThread != null) {
                localThread.interrupt();
                try {
                    localThread.join(Duration.ofSeconds(2).toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            final KafkaConsumer<String, String> localConsumer = consumer;
            if (localConsumer != null) {
                localConsumer.wakeup();
                localConsumer.close();
            }
        }
    }

    @Override
    public List<AuditEvent> listAuditEntries() {
        return new ArrayList<>(entries);
    }

    private KafkaConsumer<String, String> createConsumer() {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(properties);
    }

    private void pollLoop() {
        final KafkaConsumer<String, String> localConsumer = consumer;
        if (localConsumer == null) {
            return;
        }
        while (running.get()) {
            final ConsumerRecords<String, String> records = localConsumer.poll(POLL_TIMEOUT);
            if (records.isEmpty()) {
                continue;
            }
            for (ConsumerRecord<String, String> record : records) {
                entries.add(DPKvAuditUtils.deserialize(record.value()));
            }
            localConsumer.commitSync();
        }
    }
}
