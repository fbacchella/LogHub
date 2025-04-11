package loghub.processors;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.security.GeneralSecurityException;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import loghub.BuilderClass;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(X509Parser.Builder.class)
@FieldsProcessor.InPlace
public class X509Parser extends FieldsProcessor {

    private static final CertificateFactory X509_CERTIFICATE_FACTORY;
    static {
        try {
            X509_CERTIFICATE_FACTORY = CertificateFactory.getInstance("X.509");
        } catch (CertificateException e) {
            throw new IllegalStateException("Missing security algorithms", e);
        }
    }
    private static final Pattern MARKERS;
    static {
        String begin = "(?<begin>-+BEGIN CERTIFICATE-+)";
        String end = "(?<end>-+END CERTIFICATE-+)";
        MARKERS = Pattern.compile(String.format("(?:%s)|(?:%s)|.*?", begin, end));
    }
    private static final Base64.Decoder decoder = Base64.getDecoder();

    @Setter
    public static class Builder extends FieldsProcessor.Builder<X509Parser> {
        private boolean withExtensions = false;
        public X509Parser build() {
            return new X509Parser(this);
        }
    }

    public static X509Parser.Builder getBuilder() {
        return new X509Parser.Builder();
    }

    private final boolean withExtensions;

    private X509Parser(X509Parser.Builder builder) {
        super(builder);
        withExtensions = builder.withExtensions;
    }

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        List<X509Certificate> certs;
        try {
            if (value instanceof String) {
                certs = loadPem((String) value);
            } else if (value instanceof byte[]) {
                certs = List.of((X509Certificate) X509_CERTIFICATE_FACTORY.generateCertificate(new ByteArrayInputStream((byte[]) value)));
            } else if (value instanceof X509Certificate) {
                certs = List.of((X509Certificate) value);
            } else {
                certs = List.of();
            }
        } catch (IOException | GeneralSecurityException ex) {
           throw event.buildException("Invalid certificate field: " + Helpers.resolveThrowableException(ex), ex);
        }
        List<Map<String, Object>> extracted = certs.stream().map(m -> asJson(event, m)).collect(Collectors.toList());
        if (extracted.size() == 1) {
            return extracted.get(0);
        } else if (extracted.size() > 1){
            return extracted;
        } else {
            return RUNSTATUS.NOSTORE;
        }
    }

    private Map<String, Object> asJson(Event event, X509Certificate cert){
        // Créer un objet Map pour stocker les informations du certificat
        Map<String, Object> certMap = new HashMap<>();
        // Ajouter les informations principales
        certMap.put("subject", Map.of("distinguished_name", cert.getSubjectX500Principal().getName()));
        certMap.put("issuer", Map.of("distinguished_name", cert.getIssuerX500Principal().getName()));
        certMap.put("serial_number", cert.getSerialNumber().toString(16));  // En hexadécimal
        certMap.put("not_before", cert.getNotBefore());
        certMap.put("not_after", cert.getNotAfter());
        certMap.put("signature_algorithm", cert.getSigAlgName());
        certMap.put("version_number", cert.getVersion());
        if (cert.getBasicConstraints() >= 0) {
            certMap.put("is_root", true);
        }
        PublicKey pubKey = cert.getPublicKey();
        certMap.put("public_key_algorithm", pubKey.getAlgorithm());
        if ("RSA".equals(pubKey.getAlgorithm())) {
            RSAPublicKey rsaPublicKey = (RSAPublicKey) pubKey;
            certMap.put("public_key_size", rsaPublicKey.getModulus().bitLength());
            certMap.put("public_key_exponent", rsaPublicKey.getPublicExponent());
        } else if ("EC".equals(pubKey.getAlgorithm())) {
            ECPublicKey ecPublicKey = (ECPublicKey) pubKey;
            String curveName = ecPublicKey.getParams().toString();
            certMap.put("public_key_curve", curveName.substring(0, curveName.indexOf(' ')));
        }
        certMap.put("public_key_algorithm", pubKey.getAlgorithm());
        try {
            Collection<List<?>> subjectAlternativeName = cert.getSubjectAlternativeNames();
            if (subjectAlternativeName != null) {
                certMap.put("alternative_names", subjectAlternativeName.stream().map(this::resolveAltName).collect(Collectors.toList()));
            }
        } catch (CertificateParsingException ex) {
            throw event.wrapException("Invalid certificate field: " + Helpers.resolveThrowableException(ex), ex);
        }

        Map<String, Object> extensions = new HashMap<>();
        if (withExtensions) {
            Optional.ofNullable(cert.getCriticalExtensionOIDs()).orElse(Set.of()).forEach( oid -> {
                        Object value = resolveExtension(cert, oid);
                        if (value != null) {
                            extensions.put(oid, value);
                        }
                    }
            );
            Optional.ofNullable(cert.getCriticalExtensionOIDs()).orElse(Set.of()).forEach( oid -> {
                        Object value = resolveExtension(cert, oid);
                        if (value != null) {
                            extensions.put(oid, value);
                        }
                    }
            );
            certMap.put("extensions", extensions);
        }
        return certMap;
    }

    private Object resolveExtension(X509Certificate cert, String oid) {
        switch (oid) {
        case "2.5.29.19":
        case "2.5.29.17":
            return null;
        default:
            return Base64.getEncoder().encodeToString(cert.getExtensionValue(oid));
        }
    }

    private String resolveAltName(List<?> altName) {
        int tag = (int) altName.get(0);
        Object value = altName.get(1);
        switch (tag) {
            case 2:
                return "DNS:" +  value;
            case 7:
                return "IP:" +  value;
            default:
                return tag + ":" + value;
        }
    }

    private List<X509Certificate> loadPem(String pemCertificate) throws IOException, GeneralSecurityException {
        List<X509Certificate> certs = new ArrayList<>(1);
        pemCertificate = pemCertificate.replace("\\n", "\n");
        try (BufferedReader br = new BufferedReader(new StringReader(pemCertificate))) {
            String line;
            StringBuilder buffer = new StringBuilder();
            boolean inCertificate = false;
            while ((line = br.readLine()) != null) {
                Matcher matcher = MARKERS.matcher(line);
                matcher.matches();
                if (matcher.group("begin") != null) {
                    buffer.setLength(0);
                    inCertificate = true;
                } else if (inCertificate && matcher.group("end") != null) {
                    byte[] content = decoder.decode(buffer.toString());
                    buffer.setLength(0);
                    inCertificate = false;
                    certs.add((X509Certificate) X509_CERTIFICATE_FACTORY.generateCertificate(new ByteArrayInputStream(content)));
                } else if (inCertificate) {
                    buffer.append(line);
                }
            }
        }
        return certs;
    }

}
