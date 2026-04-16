package loghub.processors;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import loghub.BuilderClass;
import loghub.events.Event;
import lombok.Setter;

/**
 * This processor translates TLS cipher suite names between different contexts (IANA, OpenSSL, GnuTLS, Java).
 * It uses the hexadecimal identifier of the cipher suite as a pivot for translation.
 * <p>
 * The underlying YAML resource files are derived from
 * <a href="https://github.com/hcrudolph/ciphersuite.info/">https://github.com/hcrudolph/ciphersuite.info/</a>.
 * <p>
 *     It also use the content of https://www.iana.org/assignments/tls-parameters/tls-parameters.xhtml#tls-parameters-4
 * </p>
 * <p>
 *     The file openssldump.txtopenssldump.txt was generated using the command {@code openssl ciphers -V 'ALL:@SECLEVEL=0'}
 * </p>
 * <p>
 *     The GnuTLS ciphers name can be found at <a href="https://www.gnutls.org/manual/html_node/Supported-ciphersuites.html">Appendix D Supported Ciphersuites</a>
 * </p>
 */
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

    private static final Pattern CSV_PATTERN = Pattern.compile("^\"(0x[0-9A-Fa-f]{2}),(0x[0-9A-Fa-f]{2})\",(?<name>[^,]+),.*$");
    private static final Pattern OPENSSLDUMP_PATTERN = Pattern.compile("^\\s*(?<hex1>0x[0-9A-Fa-f]{2}),(?<hex2>0x[0-9A-Fa-f]{2})\\s*-\\s*(?<name>\\S+)\\s+.*$");

    private final Map<String, String> translationMap;

    record CipherId(byte byte1, byte byte2) {
        @Override
        public String toString() {
            return "0x%02x,0x%02x".formatted(byte1, byte2);
        }
    }

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

        loadIanaCsvResource(idToNames, contextToNameToId);
        loadOpenSslDumpResource(idToNames, contextToNameToId);
    }

    private void loadIanaCsvResource(Map<CipherId, Map<Context, String>> idToNames, Map<Context, Map<String, CipherId>> contextToNameToId) {
        String resource = "/ciphers/tls-parameters-4.csv";
        try (InputStream is = getClass().getResourceAsStream(resource)) {
            if (is == null) {
                logger.error("Resource not found: {}", resource);
                return;
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line;
                // Skip header
                reader.readLine();
                Set<String> keywords = Set.of("Reserved", "Unassigned");
                while ((line = reader.readLine()) != null) {
                    Matcher m = CSV_PATTERN.matcher(line);
                    if (m.matches()) {
                        String hex1 = m.group(1);
                        String hex2 = m.group(2);
                        String name = m.group("name");
                        if (keywords.contains(name)) {
                            continue;
                        }
                        store(Context.IANA, name, hex1, hex2, idToNames, contextToNameToId);
                    }
                }
            }
        } catch (IOException e) {
            logger.error("Failed to load cipher resource {}: {}", resource, e.getMessage());
        }
    }

    private void loadOpenSslDumpResource(Map<CipherId, Map<Context, String>> idToNames, Map<Context, Map<String, CipherId>> contextToNameToId) {
        String resource = "/ciphers/openssldump.txt";
        try (InputStream is = getClass().getResourceAsStream(resource)) {
            if (is == null) {
                logger.error("Resource not found: {}", resource);
                return;
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    Matcher m = OPENSSLDUMP_PATTERN.matcher(line);
                    if (m.matches()) {
                        String hex1 = m.group("hex1");
                        String hex2 = m.group("hex2");
                        String name = m.group("name");
                        store(Context.OPENSSL, name, hex1, hex2, idToNames, contextToNameToId);
                    }
                }
            }
        } catch (IOException e) {
            logger.error("Failed to load cipher resource {}: {}", resource, e.getMessage());
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

    /**
     * Used for test, not for generic use
     * @return
     */
    static Map<CipherId, Map<Context, String>> resolveId() {
        TlsCiphers cipher = getBuilder().build();
        Map<CipherId, Map<Context, String>> idToNames = new HashMap<>();
        Map<Context, Map<String, CipherId>> contextToNameToId = new EnumMap<>(Context.class);
        cipher.loadResources(idToNames, contextToNameToId);
        return idToNames;
    }

}
