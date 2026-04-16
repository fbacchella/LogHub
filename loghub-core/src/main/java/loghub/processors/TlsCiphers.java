package loghub.processors;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import loghub.BuilderClass;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(TlsCiphers.Builder.class)
public class TlsCiphers extends FieldsProcessor {

    private static final Pattern HEX_PATTERN = Pattern.compile("0x([0-9A-Fa-f]{1,2})");

    public static class Builder extends FieldsProcessor.Builder<TlsCiphers> {
        @Setter
        private String sourceContext = "iana";
        @Setter
        private String destinationContext = "openssl";

        @Override
        public TlsCiphers build() {
            return new TlsCiphers(this);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    private final String sourceContext;
    private final String destinationContext;
    private final Map<Integer, Map<String, String>> idToNames = new HashMap<>();
    private final Map<String, Map<String, Integer>> contextToNameToId = new HashMap<>();

    private TlsCiphers(Builder builder) {
        super(builder);
        this.sourceContext = builder.sourceContext.toLowerCase();
        this.destinationContext = builder.destinationContext.toLowerCase();
        loadResources();
    }

    private void loadResources() {
        String[] resources = {
            "01_iana_ciphers.yaml",
            "02_openssl_ciphers.yaml",
            "03_gnutls_ciphers.yaml",
            "04_java_ciphers.yaml"
        };
        String[] contexts = {"iana", "openssl", "gnutls", "java"};

        for (int i = 0; i < resources.length; i++) {
            String resource = "/ciphers/" + resources[i];
            String context = contexts[i];
            try (InputStream is = getClass().getResourceAsStream(resource)) {
                if (is == null) {
                    logger.error("Resource not found: {}", resource);
                    continue;
                }
                parseManualYaml(is, context);
            } catch (IOException e) {
                logger.error("Failed to load cipher resource {}: {}", resource, e.getMessage());
            }
        }
    }

    private void parseManualYaml(InputStream is, String context) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String line;
            String currentPk = null;
            String currentHex = null;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("pk:")) {
                    if (currentPk != null && currentHex != null) {
                        store(context, currentPk, currentHex);
                    }
                    currentPk = extractValue(line);
                    currentHex = null;
                } else if (line.startsWith("hex_int:")) {
                    currentHex = extractValue(line);
                }
            }
            if (currentPk != null && currentHex != null) {
                store(context, currentPk, currentHex);
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

    private void store(String context, String name, String hexStr) {
        int id = parseHex(hexStr);
        idToNames.computeIfAbsent(id, k -> new HashMap<>()).put(context, name);
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
        String cipherName = value.toString();
        Map<String, Integer> nameToId = contextToNameToId.get(sourceContext);
        if (nameToId == null) {
            return RUNSTATUS.FAILED;
        }

        Integer id = nameToId.get(cipherName);
        if (id == null) {
            return RUNSTATUS.FAILED;
        }

        Map<String, String> names = idToNames.get(id);
        if (names == null) {
            return RUNSTATUS.FAILED;
        }

        String translated = names.get(destinationContext);
        return translated != null ? translated : RUNSTATUS.FAILED;
    }
}
