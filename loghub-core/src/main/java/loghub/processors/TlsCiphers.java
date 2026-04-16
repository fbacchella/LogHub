package loghub.processors;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import loghub.BuilderClass;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(TlsCiphers.Builder.class)
public class TlsCiphers extends FieldsProcessor {

    public enum Context {
        IANA,
        OPENSSL,
        GNUTLS,
        JAVA,
    }

    public static class Builder extends FieldsProcessor.Builder<TlsCiphers> {
        @Setter
        private Context destinationContext = Context.IANA;

        @Override
        public TlsCiphers build() {
            return new TlsCiphers(this);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    private final Map<String, String> translationMap;

    private record CipherId(byte byte1, byte byte2) {}

    private TlsCiphers(Builder builder) {
        super(builder);
        Map<CipherId, Map<Context, String>> idToNames = new HashMap<>();
        Map<Context, Map<String, CipherId>> contextToNameToId = new EnumMap<>(Context.class);
        loadResources(idToNames, contextToNameToId);
        Map<String, String> localTranslationMap = new HashMap<>();
        buildTranslationMap(builder.destinationContext, idToNames, localTranslationMap);
        this.translationMap = Map.copyOf(localTranslationMap);
    }

    private void buildTranslationMap(Context destinationContext, Map<CipherId, Map<Context, String>> idToNames, Map<String, String> localTranslationMap) {
        idToNames.values().forEach(names -> {
            String targetName = names.get(destinationContext);
            if (targetName != null) {
                names.values().forEach(sourceName -> localTranslationMap.put(sourceName, targetName));
            }
        });
    }

    private void loadResources(Map<CipherId, Map<Context, String>> idToNames, Map<Context, Map<String, CipherId>> contextToNameToId) {
        Map<String, Context> resources = new HashMap<>();
        resources.put("iana_ciphers.yaml", Context.IANA);
        resources.put("openssl_ciphers.yaml", Context.OPENSSL);
        resources.put("gnutls_ciphers.yaml", Context.GNUTLS);
        resources.put("java_ciphers.yaml", Context.JAVA);

        for (Map.Entry<String, Context> entry : resources.entrySet()) {
            String resource = "/ciphers/" + entry.getKey();
            Context context = entry.getValue();
            try (InputStream is = getClass().getResourceAsStream(resource)) {
                if (is == null) {
                    logger.error("Resource not found: {}", resource);
                    continue;
                }
                parseManualYaml(is, context, idToNames, contextToNameToId);
            } catch (IOException e) {
                logger.error("Failed to load cipher resource {}: {}", resource, e.getMessage());
            }
        }
    }

    private void parseManualYaml(InputStream is, Context context, Map<CipherId, Map<Context, String>> idToNames, Map<Context, Map<String, CipherId>> contextToNameToId) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String line;
            String currentPk = null;
            String hexByte1 = null;
            String hexByte2 = null;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("pk:")) {
                    if (currentPk != null && hexByte1 != null && hexByte2 != null) {
                        store(context, currentPk, hexByte1, hexByte2, idToNames, contextToNameToId);
                    }
                    currentPk = extractValue(line);
                    hexByte1 = null;
                    hexByte2 = null;
                } else if (line.startsWith("hex_byte_1:")) {
                    hexByte1 = extractValue(line);
                } else if (line.startsWith("hex_byte_2:")) {
                    hexByte2 = extractValue(line);
                }
            }
            if (currentPk != null && hexByte1 != null && hexByte2 != null) {
                store(context, currentPk, hexByte1, hexByte2, idToNames, contextToNameToId);
            }
        }
    }

    private String extractValue(String line) {
        int colon = line.indexOf(':');
        if (colon < 0) return null;
        String val = line.substring(colon + 1).trim();
        if (val.startsWith("'") || val.startsWith("\"")) {
            val = val.substring(1, val.length() - 1);
        }
        return val;
    }

    private void store(Context context, String name, String hexStr1, String hexStr2, Map<CipherId, Map<Context, String>> idToNames, Map<Context, Map<String, CipherId>> contextToNameToId) {
        byte id1 = (byte) parseHex(hexStr1);
        byte id2 = (byte) parseHex(hexStr2);
        CipherId id = new CipherId(id1, id2);
        idToNames.computeIfAbsent(id, k -> new EnumMap<>(Context.class)).put(context, name);
        contextToNameToId.computeIfAbsent(context, k -> new HashMap<>()).put(name, id);
    }

    private int parseHex(String hex) {
        if (hex == null) return 0;
        try {
            if (hex.startsWith("0x")) {
                return Integer.parseInt(hex.substring(2), 16);
            } else {
                return Integer.parseInt(hex, 16);
            }
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public Object fieldFunction(Event event, Object value) {
        if (value == null) {
            return RUNSTATUS.FAILED;
        }
        String translated = translationMap.get(value.toString());
        return translated != null ? translated : RUNSTATUS.FAILED;
    }
}
