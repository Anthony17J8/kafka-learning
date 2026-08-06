package dev.kafkalearn.common;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/** Генератор тестовых заказов для нагрузочных и демонстрационных сценариев. */
public final class OrderGenerator {

    private static final List<String> PRODUCTS =
            List.of("laptop", "phone", "keyboard", "monitor", "mouse", "headset");

    /** Небольшой набор пользователей, чтобы ключи повторялись и было видно партиционирование. */
    private static final List<String> USERS =
            List.of("user-1", "user-2", "user-3", "user-4", "user-5");

    private OrderGenerator() {
    }

    public static OrderEvent next() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        return new OrderEvent(
                UUID.randomUUID().toString(),
                USERS.get(rnd.nextInt(USERS.size())),
                PRODUCTS.get(rnd.nextInt(PRODUCTS.size())),
                rnd.nextInt(1, 5),
                Math.round(rnd.nextDouble(10, 2000) * 100) / 100.0,
                OrderEvent.Status.CREATED,
                Instant.now()
        );
    }
}
