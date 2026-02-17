package org.esper;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.EventType;
import com.espertech.esper.runtime.client.EPEventService;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.UpdateListener;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class FileLogListener implements UpdateListener {

    private final Path outDir;
    private final int maxDepth;

    private static final ConcurrentHashMap<String, Object> FILE_LOCKS = new ConcurrentHashMap<>();

    public FileLogListener(File outDir) {
        this(outDir, 3);
    }

    public FileLogListener(File outDir, int maxDepth) {
        this.outDir = Paths.get(outDir.getPath());
        this.maxDepth = Math.max(1, maxDepth);
        try {
            Files.createDirectories(this.outDir);
        } catch (IOException e) {
            throw new RuntimeException("Cannot create output directory: " + this.outDir, e);
        }
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents, EPStatement statement, EPRuntime runtime) {
        EPEventService eventService = runtime.getEventService();
        long now;
        synchronized (eventService) {
            now = eventService.getCurrentTime();
        }

        String stmtNameRaw = statement.getName() == null ? "UnnamedStatement" : statement.getName();
        String stmtNameSafe = sanitizeFileStem(stmtNameRaw);

        Path outFile = outDir.resolve(stmtNameSafe + ".csv");
        Object lock = FILE_LOCKS.computeIfAbsent(outFile.toString(), k -> new Object());

        synchronized (lock) {
            try {
                boolean writeHeader = !Files.exists(outFile) || Files.size(outFile) == 0;

                try (BufferedWriter w = Files.newBufferedWriter(
                        outFile,
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND
                )) {
                    if (writeHeader) {
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
                throw new RuntimeException("Failed writing CSV output for statement [" + stmtNameRaw + "]", e);
            }
        }
    }

    private void writeRow(BufferedWriter w, EventBean ev, long now, String streamKind) throws IOException {
        String eventTypeName = "UnknownEventType";
        EventType et = ev.getEventType();
        if (et != null && et.getName() != null) {
            eventTypeName = et.getName();
        }

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

    /**
     * Top-level event flattening:
     *   prop=value;prop=value;...
     * IMPORTANT: nested EventBean values are rendered WITHOUT ';' separators (they use commas),
     * so the top-level split-by-';' remains stable for your comparator.
     */
    private String flattenEventDeterministic(EventBean ev) {
        EventType eventType = ev.getEventType();
        if (eventType == null) {
            return valueToDeterministicString(ev.getUnderlying(), 0);
        }

        String[] props = eventType.getPropertyNames();
        List<String> names = new ArrayList<>(Arrays.asList(props));
        Collections.sort(names);

        StringBuilder sb = new StringBuilder(256);
        boolean first = true;
        for (String pn : names) {
            if (!first) sb.append(";");
            first = false;
            Object v = ev.get(pn);
            sb.append(pn).append("=").append(valueToDeterministicString(v, 0));
        }
        return sb.toString();
    }

    private String valueToDeterministicString(Object v, int depth) {
        if (v == null) return "null";
        if (depth >= maxDepth) return "<max-depth>";

        // KEY FIX: pattern variables (e.g., "m") are nested EventBeans
        if (v instanceof EventBean eb) {
            return eventBeanToDeterministicString(eb, depth + 1);
        }

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
            return mapToDeterministicString(m, depth + 1);
        }

        return String.valueOf(v);
    }

    /**
     * Nested event serialization:
     *   TypeName{a=1,b=2,...}
     * Uses commas inside to avoid interfering with top-level ';' field separator.
     */
    private String eventBeanToDeterministicString(EventBean eb, int depth) {
        EventType et = eb.getEventType();
        String typeName = (et != null && et.getName() != null) ? et.getName() : "Event";

        if (et == null) {
            Object u = eb.getUnderlying();
            return typeName + "{" + valueToDeterministicString(u, depth + 1) + "}";
        }

        String[] props = et.getPropertyNames();
        List<String> names = new ArrayList<>(Arrays.asList(props));
        Collections.sort(names);

        StringBuilder sb = new StringBuilder();
        sb.append(typeName).append("{");
        boolean first = true;
        for (String pn : names) {
            if (!first) sb.append(",");
            first = false;
            Object pv = eb.get(pn);
            sb.append(pn).append("=").append(valueToDeterministicString(pv, depth + 1));
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Deterministic map serialization:
     *   {k1:v1,k2:v2,...}
     * Uses commas inside to avoid interfering with top-level ';' separator.
     */
    private String mapToDeterministicString(Map<?, ?> m, int depth) {
        List<String> keys = new ArrayList<>();
        for (Object k : m.keySet()) keys.add(String.valueOf(k));
        Collections.sort(keys);

        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (String k : keys) {
            if (!first) sb.append(",");
            first = false;

            Object val = null;
            for (Object ok : m.keySet()) {
                if (String.valueOf(ok).equals(k)) {
                    val = m.get(ok);
                    break;
                }
            }
            sb.append(k).append(":").append(valueToDeterministicString(val, depth + 1));
        }
        sb.append("}");
        return sb.toString();
    }

    private String csv(String s) {
        if (s == null) return "";
        boolean needs = s.contains(",") || s.contains("\"") || s.contains("\n") || s.contains("\r");
        String t = s.replace("\"", "\"\"");
        return needs ? "\"" + t + "\"" : t;
    }

    private String sanitizeFileStem(String s) {
        String t = s.trim();
        if (t.isEmpty()) return "UnnamedStatement";
        return t.replaceAll("[^A-Za-z0-9._-]+", "_");
    }
}
