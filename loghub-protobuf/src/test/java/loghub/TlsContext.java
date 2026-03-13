package loghub;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import javax.net.ssl.SSLContext;

import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolConfig.Protocol;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectedListenerFailureBehavior;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import loghub.security.ssl.SslContextBuilder;

public class TlsContext {
    public final SSLContext sslctx;
    public final SslContext nettyCtx;

    public TlsContext(Path tempDir) {
        Path tempP12File = tempDir.resolve("loghub.p12");
        try (InputStream is = TestGrpcPingServer.class.getResourceAsStream("/loghub.p12")) {
            Files.copy(is, tempP12File, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            sslctx = SslContextBuilder.getBuilder(Map.of("context", "TLSv1.3", "trusts", tempP12File.toString())).build();

            ApplicationProtocolConfig apn = new ApplicationProtocolConfig(
                    Protocol.ALPN, SelectorFailureBehavior.FATAL_ALERT, SelectedListenerFailureBehavior.FATAL_ALERT, "h2");
            nettyCtx = new JdkSslContext(sslctx, true, null, IdentityCipherSuiteFilter.INSTANCE, apn, ClientAuth.NONE, null, false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
