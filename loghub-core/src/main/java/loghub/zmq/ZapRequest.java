package loghub.zmq;

import java.util.Base64;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.zeromq.ZFrame;
import org.zeromq.ZMsg;

import lombok.Getter;
import zmq.io.mechanism.Mechanisms;

import static loghub.zmq.ZapService.ZAP_VERSION;

@Getter
public class ZapRequest {

    private final String version;   //  Version number, must be "1.0"
    private final byte[] requestId;  //  Sequence number of request
    private final String domain;    //  Server socket domain
    private final String address;   //  Client IP address
    private final String identity;  //  Server socket identity
    private final Mechanisms mechanism; //  Security mechanism
    private final String username;  //  PLAIN username
    private final String password;  //  PLAIN password, in clear text
    private final byte[] clientKey; //  CURVE client public key in ASCII
    private final String principal; //  GSSAPI principal
    private String       userId;    //  User-Id to return in the ZAP Response
    private final Map<String, String> metadata = new HashMap<>();  // metadata to eventually return

    public ZapRequest(ZMsg request) {
        version = request.popString();
        //  If the version is wrong, we're linked with a bogus libzmq, so die
        if (! ZAP_VERSION.equals(version)) {
            throw new IllegalStateException("Not supported version: " + version);
        }
        requestId = request.pop().getData();
        domain = request.popString();
        address = request.popString();
        identity = request.popString();
        mechanism = Mechanisms.valueOf(request.popString().toUpperCase(Locale.ENGLISH));

        // Get mechanism-specific frames
        if (mechanism == Mechanisms.PLAIN) {
            username = request.popString();
            password = request.popString();
            clientKey = null;
            principal = null;
        }
        else if (mechanism == Mechanisms.CURVE) {
            ZFrame frame = request.pop();
            byte[] clientPublicKey = frame.getData();
            username = null;
            password = null;
            clientKey = clientPublicKey;
            principal = null;
        }
        else if (mechanism == Mechanisms.NULL) {
            username = null;
            password = null;
            clientKey = null;
            principal = null;
        }
        else {
            throw new UnsupportedOperationException(mechanism + " not handled");
        }
    }

    public void setIdentity(String userId, Map<String, String> metadata) {
        this.userId = userId;
        this.metadata.putAll(metadata);
    }

    public void setIdentity(String userId) {
        this.userId = userId;
    }

    @Override
    public String toString() {
        return "ZapRequest [" + (version != null ? "version=" + version + ", " : "")
                       + (requestId != null ? "sequence=" + requestId + ", " : "")
                       + (domain != null ? "domain=" + domain + ", " : "")
                       + (address != null ? "address=" + address + ", " : "")
                       + (identity != null ? "identity=" + identity + ", " : "")
                       + (mechanism != null ? "mechanism=" + mechanism + ", " : "")
                       + (clientKey != null ? "clientKey=" + Base64.getEncoder().encodeToString(clientKey) + ", " : "")
                       + (userId != null ? "userId=" + userId + ", " : "")
                       + "metadata=" + metadata + "]";
    }

}
