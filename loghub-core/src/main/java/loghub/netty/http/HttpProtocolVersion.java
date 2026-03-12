package loghub.netty.http;

import java.net.http.HttpClient;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.netty.handler.codec.http.HttpVersion;

/**
 * Unified representation of HTTP protocol versions.
 *
 * <p>Provides:
 * <ul>
 *   <li>The ALPN identifier (Application-Layer Protocol Negotiation) as registered with IANA</li>
 *   <li>Resolution from {@link java.net.http.HttpClient.Version} (JDK 11+, HTTP/3 since JDK 26 — JEP 517)</li>
 *   <li>Resolution from {@link io.netty.handler.codec.http.HttpVersion} (HTTP/1.x only)</li>
 * </ul>
 *
 * <p>ALPN identifiers:
 * <ul>
 *   <li>HTTP/1.0 : no officially registered ALPN identifier</li>
 *   <li>HTTP/1.1 : {@code http/1.1} — RFC 7301</li>
 *   <li>HTTP/2   : {@code h2}       — RFC 9113 §11.1</li>
 *   <li>HTTP/3   : {@code h3}       — RFC 9114 §15.1</li>
 * </ul>
 *
 * <p><strong>HTTP/3 support:</strong> {@link #HTTP_3} is declared for completeness and
 * forward compatibility. Any operation that requires a concrete Netty or JDK artefact
 * unavailable at the current runtime will throw {@link UnsupportedOperationException}.
 */
public enum HttpProtocolVersion {

    HTTP_1_0(
            null,                 // No official ALPN identifier for HTTP/1.0
            null,                        // No HttpClient.Version constant in the JDK
            HttpVersion.HTTP_1_0
    ),

    HTTP_1_1(
            "http/1.1",
            HttpClient.Version.HTTP_1_1,
            HttpVersion.HTTP_1_1
    ),

    HTTP_2(
            "h2",
            HttpClient.Version.HTTP_2,
            null
    ),

    /**
     * HTTP/3 over QUIC — RFC 9114.
     *
     * <p>The JDK constant ({@code HttpClient.Version.HTTP_3}) is available since JDK 26 (JEP 517).
     * Netty HTTP/3 support lives in {@code netty-incubator-codec-http3} and is not represented
     * by {@link io.netty.handler.codec.http.HttpVersion}.
     *
     * <p>Calling {@link #toNettyVersion()} on this constant always throws
     * {@link UnsupportedOperationException}. Calling {@link #toJdkVersion()} throws
     * {@link UnsupportedOperationException} when running on JDK &lt; 26.
     */
    HTTP_3(
            "h3",
            resolveHttp3JdkVersion(),    // null when running on JDK < 26
            null                         // No Netty HttpVersion for HTTP/3
    );

    /**
     * ALPN identifier as registered with IANA.
     * {@code null} for HTTP/1.0, which predates ALPN and has no registered entry.
     */
    private final String alpnId;

    /**
     * Corresponding {@link java.net.http.HttpClient.Version} constant.
     * {@code null} when the JDK does not model this protocol version
     * (HTTP/1.0, or HTTP/3 on JDK &lt; 26).
     */
    private final HttpClient.Version jdkVersion;

    /**
     * Corresponding {@link io.netty.handler.codec.http.HttpVersion} constant.
     * {@code null} for HTTP/2 and HTTP/3, which are not represented by that Netty class.
     */
    private final HttpVersion nettyVersion;

    private static final Map<String, HttpProtocolVersion> BY_ALPN_ID;
    private static final Map<HttpClient.Version, HttpProtocolVersion> BY_JDK_VERSION;
    private static final Map<HttpVersion, HttpProtocolVersion> BY_NETTY_VERSION;

    static {
        BY_ALPN_ID = Arrays.stream(values())
                           .filter(v -> v.alpnId != null)
                           .collect(Collectors.toUnmodifiableMap(v -> v.alpnId, Function.identity()));
        BY_JDK_VERSION = Arrays.stream(values())
                               .filter(v -> v.jdkVersion != null)
                               .collect(Collectors.toUnmodifiableMap(v -> v.jdkVersion, Function.identity()));
        BY_NETTY_VERSION = Arrays.stream(values())
                                 .filter(v -> v.nettyVersion != null)
                                 .collect(Collectors.toUnmodifiableMap(v -> v.nettyVersion, Function.identity()));
    }

    HttpProtocolVersion(
            String alpnId,
            HttpClient.Version jdkVersion,
            HttpVersion nettyVersion
    ) {
        this.alpnId         = alpnId;
        this.jdkVersion     = jdkVersion;
        this.nettyVersion   = nettyVersion;
    }

    /**
     * Returns the ALPN identifier for this version, or {@link Optional#empty()}
     * if no official identifier exists (HTTP/1.0).
     */
    public Optional<String> alpnId() {
        return Optional.ofNullable(alpnId);
    }

    /**
     * Returns the corresponding {@link HttpClient.Version} constant.
     *
     * @return the JDK version constant
     * @throws UnsupportedOperationException if this version has no JDK mapping
     *         (HTTP/1.0, or HTTP/3 on JDK &lt; 26)
     */
    public HttpClient.Version toJdkVersion() {
        if (jdkVersion == null) {
            throw new UnsupportedOperationException(
                    "No JDK HttpClient.Version mapping for " + this
                            + (this == HTTP_3 ? " — requires JDK 26+ (JEP 517)" : ""));
        }
        return jdkVersion;
    }

    /**
     * Returns the corresponding Netty {@link HttpVersion} constant.
     *
     * <p>The comparison uses {@code ==} because Netty guarantees reference
     * interning for {@code HTTP_1_0} and {@code HTTP_1_1}.
     *
     * @return the Netty version constant
     * @throws UnsupportedOperationException if this version has no Netty {@link HttpVersion}
     *         mapping (HTTP/2 and HTTP/3 — handled by separate Netty codec modules)
     */
    public HttpVersion toNettyVersion() {
        if (nettyVersion == null) {
            throw new UnsupportedOperationException(
                    "No Netty HttpVersion mapping for " + this
                            + switch (this) {
                        case HTTP_2 -> " — use netty-codec-http2 (io.netty.handler.codec.http2)";
                        case HTTP_3 -> " — use netty-incubator-codec-http3";
                        default     -> "";
                    });
        } else {
            return nettyVersion;
        }
    }

    // -------------------------------------------------------------------------
    // Resolution
    // -------------------------------------------------------------------------

    /**
     * Resolves from an ALPN identifier.
     *
     * @param alpnId ALPN identifier (e.g. {@code "h2"}, {@code "http/1.1"})
     * @return the matching version, or {@link Optional#empty()} if unknown
     */
    public static Optional<HttpProtocolVersion> fromAlpnId(String alpnId) {
        return alpnId == null ? Optional.empty() : Optional.ofNullable(BY_ALPN_ID.get(alpnId));
    }

    /**
     * Resolves from a JDK {@link HttpClient.Version} constant.
     *
     * @param version JDK constant, must not be null
     * @return the matching version
     * @throws IllegalArgumentException if the JDK constant is not recognised
     */
    public static HttpProtocolVersion fromJdkVersion(HttpClient.Version version) {
        if (version == null) {
            throw new IllegalArgumentException("JDK version must not be null");
        }
        HttpProtocolVersion resolved = BY_JDK_VERSION.get(version);
        if (resolved != null) {
            return resolved;
        } else {
            throw new IllegalArgumentException("Unrecognised JDK version: " + version);
        }
    }

    /**
     * Resolves from a Netty {@link HttpVersion} constant.
     *
     * <p>The comparison uses {@code ==} because Netty guarantees reference
     * interning for {@code HTTP_1_0} and {@code HTTP_1_1}. An {@link HttpVersion}
     * instance constructed directly via {@code new HttpVersion(...)} would not
     * match any entry; this is intentional.
     *
     * @param version Netty constant, must not be null
     * @return the matching version
     * @throws IllegalArgumentException if the Netty constant is not recognised
     */
    public static HttpProtocolVersion fromNettyVersion(HttpVersion version) {
        if (version == null) {
            throw new IllegalArgumentException("Netty version must not be null");
        }
        HttpProtocolVersion resolved = BY_NETTY_VERSION.get(version);
        if (resolved != null) {
            return resolved;
        } else {
            throw new IllegalArgumentException("Unrecognised Netty version: " + version);
        }
    }

    /**
     * Resolves {@code HttpClient.Version.HTTP_3} reflectively so that this enum
     * compiles and loads on JDK 11–25 without a hard dependency on a constant
     * that does not exist before JDK 26.
     *
     * @return the {@code HTTP_3} constant, or {@code null} on JDK &lt; 26
     */
    private static HttpClient.Version resolveHttp3JdkVersion() {
        try {
            return (HttpClient.Version) HttpClient.Version.class
                                                  .getField("HTTP_3")
                                                  .get(null);
        } catch (NoSuchFieldException e) {
            return null; // JDK < 26 — HTTP_3 entry will have no JDK mapping
        } catch (IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

}
