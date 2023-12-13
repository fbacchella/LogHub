package loghub.receivers;

import loghub.BuilderClass;
import loghub.Helpers;
import loghub.decoders.StringCodec;

@BuilderClass(TcpLinesStream.Builder.class)
public class TcpLinesStream extends AbstractNettyStream<TcpLinesStream, TcpLinesStream.Builder> {

    public static class Builder extends AbstractNettyStream.Builder<TcpLinesStream, TcpLinesStream.Builder> {
        public Builder() {
            super();
            // A ready to use TcpLinesStream: single line text message.
            StringCodec.Builder sbuilder = new StringCodec.Builder();
            setDecoder(sbuilder.build());
        }
        @Override
        public TcpLinesStream build() {
            return new TcpLinesStream(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private TcpLinesStream(Builder builder) {
        super(builder);
    }

    @Override
    protected String getThreadPrefix(Builder builder) {
        return "LineReceiver";
    }

    @Override
    public String getReceiverName() {
        return "LineReceiver/" + Helpers.ListenString(getListen()) + "/" + getPort();
    }

}
