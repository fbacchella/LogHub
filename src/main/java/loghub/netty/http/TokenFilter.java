package loghub.netty.http;

import loghub.security.AuthenticationHandler;

@RequestAccept(path="/token", methods={"GET"})
public class TokenFilter extends AccessControl {

    public TokenFilter(AuthenticationHandler authhandler) {
        super(authhandler);
    }

}
