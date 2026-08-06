package dev.kafkalearn.producers;

import dev.kafkalearn.common.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class FailoverProducer {

    private static final String TOPIC = KafkaConfig.topic("failover-test");

    public static void main(String[] args) throws Exception {
        final AtomicLong beforeSend = new AtomicLong(0L);
        final AtomicLong acked = new AtomicLong(0L);
        final AtomicLong errors = new AtomicLong(0L);

        Thread t = logDaemon(beforeSend, acked, errors);
        t.start();

        CountDownLatch latch = new CountDownLatch(1);
        LogResult logHook = new LogResult(beforeSend, acked, errors, latch);
        Runtime.getRuntime().addShutdownHook(logHook);

        Properties properties = producerProps();
        StringSerializer sSer = new StringSerializer();
        try (Producer<String, String> producer = new KafkaProducer<>(properties, sSer, sSer)) {
            while (!logHook.getStopSIG()) {
                Thread.sleep(2);
                long l = beforeSend.incrementAndGet();
                producer.send(new ProducerRecord<>(TOPIC, "key-" + (l % 5), UUID.randomUUID().toString()),
                    (metadata, exception) -> {
                        if (exception != null) {
                            errors.incrementAndGet();
                        } else {
                            acked.incrementAndGet();
                        }
                    });
            }
            producer.flush();
        }
        latch.countDown();
    }

    private static Thread logDaemon(AtomicLong beforeSend, AtomicLong acked, AtomicLong errors) {
        Thread t = new Thread(() -> {
            long counter = 0;
            while (true) {
                try {
                    Thread.sleep(1000);
                    long delta = acked.get() - counter;
                    counter = acked.get();
                    System.out.printf("""
                        %d - количество сообщений перед отправкой продюсером
                        %d - количество сообщений подтверждено
                        %d - количество ошибок при отправке
                        %d - N/sec
                        \n""", beforeSend.get(), acked.get(), errors.get(), delta);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        t.setDaemon(true);
        return t;
    }

    /**
     * Конфигурация продюсера. Каждая опция — предмет отдельного эксперимента
     */
    static Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "failover-producer");

        String acks = System.getProperty("acks", "all");
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, acks.equals("all"));
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        return props;
    }

    static class LogResult extends Thread {
        private final AtomicLong beforeSend;
        private final AtomicLong acked;
        private final AtomicLong errors;
        private final CountDownLatch latch;

        private volatile boolean stopSIG = false;

        LogResult(AtomicLong beforeSend,
                  AtomicLong acked,
                  AtomicLong errors,
                  CountDownLatch latch) {
            super();
            this.beforeSend = beforeSend;
            this.acked = acked;
            this.errors = errors;
            this.latch = latch;
        }

        public void setStopSIG(boolean stopSIG) {
            this.stopSIG = stopSIG;
        }

        public boolean getStopSIG() {
            return stopSIG;
        }

        @Override
        public void run() {
            setStopSIG(true);
            try {
                latch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            System.out.println("==============RESULT=============");
            System.out.printf("""
                %d - количество сообщений перед отправкой продюсером
                %d - количество сообщений подтверждено
                %d - количество ошибок при отправке
                """, beforeSend.get(), acked.get(), errors.get());

        }
    }
}
