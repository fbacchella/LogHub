package loghub.receivers;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.string.StringDecoder;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.events.Event;
import loghub.netty.NettyReceiver;
import loghub.netty.transport.TRANSPORT;
import lombok.Setter;

@BuilderClass(MultiLinesStream.Builder.class)
@SelfDecoder
public class MultiLinesStream extends AbstractNettyStream<MultiLinesStream, MultiLinesStream.Builder> {

    private class MergeMessage extends MessageToMessageDecoder<String> {
        private final StringBuilder buffer = new StringBuilder();

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);
            if (buffer.length() > 0) {
                MultiLinesStream.this.send(makeEvent(ctx));
            }
        }

        /**
         * Decode from one message to another. This method will be called for each written message that can be handled
         * by this decoder.
         *
         * @param ctx the {@link ChannelHandlerContext} which this {@link MessageToMessageDecoder} belongs to
         * @param msg the message to decode to another one
         * @param out the {@link List} to which decoded messages should be added
         */
        @Override
        protected void decode(ChannelHandlerContext ctx, String msg, List<Object> out) {
            if (matcherHolder.get().reset(msg).matches()){
                buffer.append(joinWith).append(matcherHolder.get().group("payload"));
            } else {
                if (buffer.length() > 0) {
                    out.add(makeEvent(ctx));
                }
                buffer.append(msg);
            }
            matcherHolder.get().reset("");
        }

        private Event makeEvent(ChannelHandlerContext ctx) {
            ConnectionContext<?> cctx = ctx.channel().attr(NettyReceiver.CONNECTIONCONTEXTATTRIBUTE).get();
            Event ev = MultiLinesStream.this.mapToEvent(cctx, Map.of("message", buffer.toString()));
            buffer.setLength(0);
            return ev;
        }

    }

    public static class Builder extends AbstractNettyStream.Builder<MultiLinesStream, MultiLinesStream.Builder> {
        public Builder() {
            super();
            setTransport(TRANSPORT.TCP);
        }
        @Setter
        private String charset = Charset.defaultCharset().name();
        @Setter
        private String joinWith = null;
        @Setter
        private String mergePattern = " +(?<payload>.*)";
        @Override
        public MultiLinesStream build() {
            return new MultiLinesStream(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private final StringDecoder messageDecoder;
    private final ThreadLocal<Matcher> matcherHolder;
    private final String joinWith;

    private MultiLinesStream(Builder builder) {
        super(builder);
        Pattern matcherPattern = Pattern.compile(builder.mergePattern);
        matcherHolder = ThreadLocal.withInitial(() -> matcherPattern.matcher(""));
        Charset charset = Charset.forName(builder.charset);
        messageDecoder = new StringDecoder(charset);
        joinWith = Optional.ofNullable(builder.joinWith).or(() -> Optional.ofNullable(builder.separator)).orElse("\n");
    }

    @Override
    protected String getThreadPrefix(Builder builder) {
        return "MultiLineReceiver";
    }

    @Override
    protected void updateHandler(ChannelPipeline pipe) {
        if (matcherHolder != null) {
            pipe.addAfter("Splitter", "StringDecoder", messageDecoder);
            MergeMessage merger = new MergeMessage();
            pipe.addAfter("StringDecoder", "CheckMerger", merger);
        }
    }

    @Override
    public String getReceiverName() {
        return "MultiLineReceiver/" + Helpers.ListenString(getListen()) + "/" + getPort();
    }

}
