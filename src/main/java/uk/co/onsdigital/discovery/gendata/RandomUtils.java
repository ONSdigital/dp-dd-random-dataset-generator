package uk.co.onsdigital.discovery.gendata;

import java.util.Base64;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Utilities for generating random data.
 */
final class RandomUtils {
    private RandomUtils() {}

    static String randomString(String prefix, String suffix) {
        return prefix + randomString() + suffix;
    }

    static String randomString() {
        final byte[] data = new byte[16];
        ThreadLocalRandom.current().nextBytes(data);
        return Base64.getUrlEncoder().encodeToString(data).replace("=", "-");
    }

    static long randomLong() {
        return ThreadLocalRandom.current().nextLong();
    }

    static String randomYear() {
        return Integer.toString(1000 + ThreadLocalRandom.current().nextInt(2000));
    }

    static String randomElement(List<String> choices) {
        return choices.get(ThreadLocalRandom.current().nextInt(choices.size()));
    }
}
