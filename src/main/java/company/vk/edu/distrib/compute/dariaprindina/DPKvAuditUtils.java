package company.vk.edu.distrib.compute.dariaprindina;

import company.vk.edu.distrib.compute.AuditEvent;

final class DPKvAuditUtils {
    private static final String SEPARATOR = "\t";

    private DPKvAuditUtils() {
    }

    static String serialize(String method, String id, long timestamp) {
        return method + SEPARATOR + id + SEPARATOR + timestamp;
    }

    static AuditEvent deserialize(String payload) {
        final String[] parts = payload.split(SEPARATOR, 3);
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid audit payload");
        }
        return new AuditEvent(parts[0], parts[1], Long.parseLong(parts[2]));
    }
}
