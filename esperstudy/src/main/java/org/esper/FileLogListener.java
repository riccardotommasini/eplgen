package org.esper;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.EventType;
import com.espertech.esper.common.internal.event.arr.ObjectArrayEventType;
import com.espertech.esper.common.internal.event.map.MapEventType;
import com.espertech.esper.runtime.client.EPEventService;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.UpdateListener;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class FileLogListener implements UpdateListener {

    private Path outpath;

    private static final ConcurrentHashMap<String, Object> FILE_LOCKS = new ConcurrentHashMap<>();

    public FileLogListener(String outDir) {
        this.outpath = Path.of(outDir);
        try {
            Files.createDirectories(outpath);
        } catch (IOException e) {
            throw new RuntimeException("Cannot create output directory: " + outDir, e);
        }
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents, EPStatement statement, EPRuntime runtime) {
        EPEventService eventService = runtime.getEventService();

        String stmtNameRaw = statement.getName() == null ? "UnnamedStatement" : statement.getName();
        String stmtNameSafe = sanitizeFileStem(stmtNameRaw);
        Path outFile = outpath.resolve(stmtNameSafe + ".csv");

        Object lock = FILE_LOCKS.computeIfAbsent(outFile.toString(), k -> new Object());

        synchronized (lock) {
            synchronized (eventService) {
                long now = eventService.getCurrentTime();

                try {
                    boolean writeHeader = !Files.exists(outFile) || Files.size(outFile) == 0;

                    try (BufferedWriter w = Files.newBufferedWriter(
                            outFile,
                            StandardCharsets.UTF_8,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.APPEND
                    )) {
                        if (writeHeader) {
                            // Minimal stable schema for fast comparison.
                            // EventType: Esper event type name (or underlying type)
                            // Timestamp: runtime current time
                            // Stream: NEW or OLD
                            // Fields: deterministic key=value;... representation
                            w.write("EventType,Timestamp,Stream,Fields");
                            w.newLine();
                        }

                        if (newEvents != null) {
                            for (EventBean ev : newEvents) {
                                writeRow(w, ev, now, "NEW");
                            }
                        }
                        if (oldEvents != null) {
                            for (EventBean ev : oldEvents) {
                                writeRow(w, ev, now, "OLD");
                            }
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Failed writing CSV output for statement [" + stmtNameRaw + "] to " + outFile, e);
                }
            }
        }
    }

    private static void writeRow(BufferedWriter w, EventBean ev, long now, String streamKind) throws IOException {
        EventType et = ev.getEventType();
        String eventTypeName = (et != null && et.getName() != null) ? et.getName() : "UnknownEventType";

        String fields = flattenEventDeterministic(ev);

        w.write(csv(eventTypeName));
        w.write(",");
        w.write(csv(String.valueOf(now)));
        w.write(",");
        w.write(csv(streamKind));
        w.write(",");
        w.write(csv(fields));
        w.newLine();
    }

    private static String flattenEventDeterministic(EventBean ev) {
        EventType eventType = ev.getEventType();

        // Prefer properties when we have them.
        if (eventType instanceof ObjectArrayEventType || eventType instanceof MapEventType) {
            String[] props = eventType.getPropertyNames();
            // keep the property order stable, but also ensure deterministic map rendering
            StringBuilder sb = new StringBuilder(256);
            for (int i = 0; i < props.length; i++) {
                String pn = props[i];
                Object v = ev.get(pn);
                if (i > 0) sb.append(";");
                sb.append(pn).append("=").append(valueToDeterministicString(v));
            }
            return sb.toString();
        }

        // Fallback: underlying object
        Object u = ev.getUnderlying();
        return valueToDeterministicString(u);
    }

    private static String valueToDeterministicString(Object v) {
        if (v == null) return "null";

        if (v instanceof String[]) return Arrays.deepToString((String[]) v);
        if (v instanceof String[][]) return Arrays.deepToString((String[][]) v);
        if (v instanceof Object[]) return Arrays.deepToString((Object[]) v);

        if (v.getClass().isArray()) {
            if (v instanceof int[]) return Arrays.toString((int[]) v);
            if (v instanceof long[]) return Arrays.toString((long[]) v);
            if (v instanceof double[]) return Arrays.toString((double[]) v);
            if (v instanceof float[]) return Arrays.toString((float[]) v);
            if (v instanceof boolean[]) return Arrays.toString((boolean[]) v);
            if (v instanceof byte[]) return Arrays.toString((byte[]) v);
            if (v instanceof short[]) return Arrays.toString((short[]) v);
            if (v instanceof char[]) return Arrays.toString((char[]) v);
        }

        if (v instanceof Map<?, ?> m) {
            return mapToDeterministicString(m);
        }

        return String.valueOf(v);
    }

    private static String mapToDeterministicString(Map<?, ?> m) {
        // Sort keys for deterministic output across JVM runs/HashMap order.
        List<String> keys = new ArrayList<>();
        for (Object k : m.keySet()) keys.add(String.valueOf(k));
        Collections.sort(keys);

        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (String k : keys) {
            if (!first) sb.append(",");
            first = false;

            Object val = null;
            // Find original key object matching this string (rare collisions acceptable for logs).
            for (Object ok : m.keySet()) {
                if (String.valueOf(ok).equals(k)) {
                    val = m.get(ok);
                    break;
                }
            }
            sb.append(k).append(":").append(valueToDeterministicString(val));
        }
        sb.append("}");
        return sb.toString();
    }

    private static String csv(String s) {
        // RFC4180-style quoting: quote if needed, escape " as "".
        if (s == null) return "";
        boolean needs = s.contains(",") || s.contains("\"") || s.contains("\n") || s.contains("\r");
        String t = s.replace("\"", "\"\"");
        return needs ? "\"" + t + "\"" : t;
    }

    private static String sanitizeFileStem(String s) {
        String t = s.trim();
        if (t.isEmpty()) return "UnnamedStatement";
        return t.replaceAll("[^A-Za-z0-9._-]+", "_");
    }
}
