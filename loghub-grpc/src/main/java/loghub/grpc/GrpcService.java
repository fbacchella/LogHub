package loghub.grpc;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;

import io.netty.buffer.ByteBuf;
import loghub.grpc.BinaryCodec.UnknownField;
import lombok.Getter;

public abstract class GrpcService<I, O> {

    public static final Object CLOSE_EVENT = new Object();

    private static class SimpleGrpcService<I, O> extends GrpcService<I, O> {
        private final Function<I, O> processor;
        public SimpleGrpcService(BinaryCodec codec, String methodName, Function<I, O> processor) {
            super(codec, methodName);
            this.processor = processor;
        }

        @Override
        public O doProcessing(I request) {
            return processor.apply(request);
        }
    }

    @Getter
    protected final BinaryCodec codec;
    @Getter
    protected final MethodDescriptor method;

    protected GrpcService(BinaryCodec codec, String methodName) {
        this.codec = codec;
        this.method = codec.getMethodDescriptor(methodName);
    }

    protected GrpcService(BinaryCodec codec,
            MethodDescriptor method) {
        this.codec = codec;
        this.method = method;
    }
    public I decodeRequest(ByteBuf messageBuf, List<UnknownField> unknownFields) throws IOException {
        Descriptor imd = method.getInputType();
        return codec.decode(CodedInputStream.newInstance(messageBuf.nioBuffer()), imd, unknownFields);
    }
    public byte[] encodeResponse(Map<String, Object> m) {
        Descriptor omd = method.getOutputType();
        return codec.encode(omd, m).toByteArray();
    }
    public byte[] encodeResponse(Object o) {
        return codec.encode(o).orElseThrow(() -> new IllegalArgumentException(
                "Method " + getQualifiedMethodName() + " returned unexpected type: " + o.getClass().getName()));
    }
    public abstract O doProcessing(I request) throws GrpcMethodException;
    public void onClose() {

    }

    public String getQualifiedMethodName() {
        return method.getFullName();
    }

    public static <I, O> GrpcService<I, O> of(BinaryCodec codec, String methodName, Function<I, O> processor) {
        return new SimpleGrpcService(codec, methodName, processor);
    }

    public static <I, O> GrpcService<I, O> of(BinaryCodec codec, String methodName, Function<I, O> processor, Runnable onClose) {
        return new GrpcService<>(codec, methodName) {
            @Override
            public O doProcessing(I request) {
                return processor.apply(request);
            }

            @Override
            public void onClose() {
                onClose.run();
            }
        };
    }

}
