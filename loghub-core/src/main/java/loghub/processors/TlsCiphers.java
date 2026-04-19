package loghub.processors;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import loghub.BuilderClass;
import loghub.events.Event;
import lombok.Getter;
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
 *     The GnuTLS ciphers name can be found at <a href="https://www.gnutls.org/manual/html_node/Supported-ciphersuites.html">Appendix D Supported Ciphersuites</a>
 * </p>
 * <p>
 *     The OpenSSL ciphers name can be found at <a href="https://github.com/openssl/openssl/blob/master/include/openssl/tls1.h">openssl/include/openssl/tls1.h</a>
 * </p>
 * <p>
 *     The Java ciphers were extracted from the Enum sun.security.ssl.CipherSuite
 * </p>
 */
@BuilderClass(TlsCiphers.Builder.class)
public class TlsCiphers extends FieldsProcessor {

    public enum Context {
        IANA("directory.IanaCipher"),
        OPENSSL("directory.OpensslCipher"),
        GNUTLS("directory.GnutlsCipher"),
        JAVA("directory.JavaCipher"),
        CUSTOM("directory.Custom"),
        ;
        @Getter
        private final String modelName;

        Context(String modelName) {
            this.modelName = modelName;
        }

        static Context resolve(String context) {
            switch (context) {
                case "directory.GnutlsCipher" -> {
                return Context.GNUTLS;
            }
            case "directory.JavaCipher" -> {
                return Context.JAVA;
            }
            case "directory.OpensslCipher" -> {
                return Context.OPENSSL;
            }
            case "directory.IanaCipher" -> {
                return Context.IANA;
            }
            case "directory.Custom" -> {
                return Context.CUSTOM;
            }
            default -> throw new IllegalArgumentException(context);
            }

        }
    }

    public static class Builder extends FieldsProcessor.Builder<TlsCiphers> {
        @Setter
        private URI[] extensions = new URI[0];
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
        for (URI u: builder.extensions) {
            try {
                readYamlUrl(u.toURL(), idToNames, contextToNameToId);
            } catch (MalformedURLException e) {
                logger.error("Unusable ciphers names URI \"{}\"", u);
            }
        }

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
        for (String yaml: List.of("openssl_ciphers.yaml", "gnutls_ciphers.yaml", "java_ciphers.yaml")) {
            String resource = "/ciphers/" + yaml;
            readYamlUrl(getClass().getResource(resource), idToNames, contextToNameToId);
        }
        loadIanaCsvResource(idToNames, contextToNameToId);
    }

    private void readYamlUrl(URL yaml, Map<CipherId, Map<Context, String>> idToNames, Map<Context, Map<String, CipherId>> contextToNameToId) {
        try (InputStream is = yaml.openStream()) {
            if (is == null) {
                logger.error("Resource not found: {}", yaml);
            }
            parseManualYaml(is, idToNames, contextToNameToId);
        } catch (IOException e) {
            logger.error("Failed to load cipher resource {}: {}", yaml, e.getMessage());
        }
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
                        store(Context.IANA.getModelName(), name, hex1, hex2, idToNames, contextToNameToId);
                    }
                }
            }
        } catch (IOException e) {
            logger.error("Failed to load cipher resource {}: {}", resource, e.getMessage());
        }
    }

    private void parseManualYaml(InputStream is, Map<CipherId, Map<Context, String>> idToNames, Map<Context, Map<String, CipherId>> contextToNameToId) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String line;
            String model = null;
            String currentPk = null;
            String hexByte1 = null;
            String hexByte2 = null;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("- ")) {
                    if (model != null && currentPk != null && hexByte1 != null && hexByte2 != null) {
                        store(model, currentPk, hexByte1, hexByte2, idToNames, contextToNameToId);
                    }
                    model = null;
                    currentPk = null;
                    hexByte1 = null;
                    hexByte2 = null;
                    line = line.substring(2);
                }
                if (line.startsWith("pk:")) {
                    currentPk = extractValue(line);
                } else if (line.startsWith("hex_byte_1:")) {
                    hexByte1 = extractValue(line);
                } else if (line.startsWith("hex_byte_2:")) {
                    hexByte2 = extractValue(line);
                } else if (line.startsWith("model:")) {
                    model = extractValue(line);
                }
            }
            // Handle the last entry
            if (model != null && currentPk != null && hexByte1 != null && hexByte2 != null) {
                store(model, currentPk, hexByte1, hexByte2, idToNames, contextToNameToId);
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

    private void store(String model, String name, String hexStr1, String hexStr2, Map<CipherId, Map<Context, String>> idToNames, Map<Context, Map<String, CipherId>> contextToNameToId) {
        byte id1 = Optional.ofNullable(hexStr1).map(this::parseHex).orElseThrow(() -> new IllegalArgumentException("Missing byte1"));
        byte id2 = Optional.ofNullable(hexStr2).map(this::parseHex).orElseThrow(() -> new IllegalArgumentException("Missing byte2"));
        CipherId id = new CipherId(id1, id2);
        Context context = Context.resolve(model);
        idToNames.computeIfAbsent(id, k -> new EnumMap<>(Context.class)).put(context, name);
        contextToNameToId.computeIfAbsent(Context.resolve(model), k -> new HashMap<>()).put(name, id);
    }

    private byte parseHex(String hex) {
        int value;
        try {
            if (hex.startsWith("0x")) {
                value = Integer.parseInt(hex.substring(2), 16);
            } else {
                value = Integer.parseInt(hex, 16);
            }
            if ((value & ~0xFF) != 0) {
                throw new ArithmeticException("Overflow of value %s".formatted(hex));
            } else {
                return (byte) value;
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(hex, e);
        }
    }

    @Override
    public Object fieldFunction(Event event, Object value) {
        if (value == null) {
            return RUNSTATUS.FAILED;
        } else {
            String translated = translationMap.get(value.toString());
            return translated != null ? translated : RUNSTATUS.FAILED;
        }
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
