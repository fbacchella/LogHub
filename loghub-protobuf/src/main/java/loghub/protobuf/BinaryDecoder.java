package loghub.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.protobuf.Any;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Type;

import lombok.Data;

public class BinaryDecoder {

    private final Map<String, Map<Integer, Descriptors.FieldDescriptor>> descriptors;
    private final Map<String, FastPathFunction<?>> fastPathMap = new HashMap<>();
    private final Map<String, Descriptors.Descriptor> messages = new HashMap<>();
    private final Map<String, Descriptors.MethodDescriptor> methods = new HashMap<>();

    @FunctionalInterface
    public interface FastPathFunction<T> {
        T resolve(CodedInputStream stream, List<UnknownField> unknownFields) throws IOException;
    }

    public BinaryDecoder(URI source) throws Descriptors.DescriptorValidationException, IOException {
        try (InputStream is = source.toURL().openStream()) {
            descriptors = analyseProto(is);
        }
        initFastPath();
    }

    public BinaryDecoder(InputStream source) throws Descriptors.DescriptorValidationException, IOException {
        if (source == null) {
            throw new IllegalArgumentException("Not defined InputStream source");
        }
        descriptors = analyseProto(source);
        initFastPath();
    }

    public BinaryDecoder(Path source) throws Descriptors.DescriptorValidationException, IOException {
        try (InputStream is = Files.newInputStream(source)) {
            descriptors = analyseProto(is);
        }
        initFastPath();
    }

    protected void initFastPath() {
        fastPathMap.put("com.google.protobuf.Any", (s, u) -> Any.parseFrom(s.readByteBuffer()));
        fastPathMap.put("com.google.protobuf.Duration", (s, u) -> {
            Duration d = Duration.parseFrom(s.readByteBuffer());
            return java.time.Duration.ofSeconds(d.getSeconds(), d.getNanos());
        });
        fastPathMap.put("com.google.protobuf.Timestamp", (s, u) -> {
            Timestamp ts = Timestamp.parseFrom(s.readByteBuffer());
            return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
        });
    }

    private Map<String, Map<Integer, Descriptors.FieldDescriptor>> analyseProto(InputStream source)
            throws IOException, Descriptors.DescriptorValidationException {
        Map<String, Map<Integer, Descriptors.FieldDescriptor>> current = new HashMap<>();
        for (Descriptors.FileDescriptor fd : resolveProto(source)) {
            for (Descriptors.Descriptor dd : fd.getMessageTypes()) {
                scanDescriptor(dd, current);
                messages.put(dd.getFullName(), dd);
            }
            for (var sd : fd.getServices()) {
                for (var md: sd.getMethods()) {
                    methods.put(md.getFullName(), md);
                }
            }
        }
        return current;
    }

    private List<Descriptors.FileDescriptor> resolveProto(InputStream source)
            throws IOException, Descriptors.DescriptorValidationException {
        List<Descriptors.FileDescriptor> dependencies = new ArrayList<>();
        List<Descriptors.FileDescriptor> files = new ArrayList<>();
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(source);
        for (DescriptorProtos.FileDescriptorProto fdp : descriptorSet.getFileList()) {
            Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(fdp,
                    dependencies.toArray(Descriptors.FileDescriptor[]::new));
            files.add(fd);
            dependencies.add(fd);
        }
        return files;
    }

    private void scanDescriptor(Descriptors.Descriptor dd,
            Map<String, Map<Integer, Descriptors.FieldDescriptor>> descriptors) {
        dd.getNestedTypes().forEach(d -> scanDescriptor(d, descriptors));
        for (Descriptors.FieldDescriptor dfd : dd.getFields()) {
            if (dfd.isExtension()) {
                throw new UnsupportedOperationException("Extensions are no supported");
            } else {
                descriptors.computeIfAbsent(dd.getFullName(), k -> new HashMap<>()).put(dfd.getNumber(), dfd);
            }
        }
    }

    public <T> void addFastPath(String attributeFullName, FastPathFunction<T> fastPath) {
        if (fastPath == null) {
            fastPathMap.remove(attributeFullName);
        } else {
            fastPathMap.put(attributeFullName, fastPath);
        }
    }

    @Data
    public static class UnknownField {
        private final String message;
        private final int fieldNumber;
        private final int fieldWireType;
        private final Object value;
    }

    public Map<String, Object> parseInput(CodedInputStream stream, String messageName, List<UnknownField> unknownFields) throws IOException {
        return parseMessage(stream, messages.get(messageName), unknownFields);
    }

    @SuppressWarnings("unchecked")
    private <T> T parseMessage(CodedInputStream stream, Descriptors.Descriptor descriptor, List<UnknownField> unknownFields)
            throws IOException {
        if (fastPathMap.containsKey(descriptor.getFullName())) {
            return (T) fastPathMap.get(descriptor.getFullName()).resolve(stream, unknownFields);
        } else {
            Map<String, Object> values = new HashMap<>();
            Set<Descriptors.FieldDescriptor> expected = new HashSet<>(descriptor.getFields());
            while (!stream.isAtEnd()) {
                int tag = stream.readTag();
                int fieldNumber = (tag >> 3);
                Descriptors.FieldDescriptor desc = descriptor.findFieldByNumber(fieldNumber);
                expected.remove(desc);
                if (desc != null) {
                    if (desc.isRepeated()) {
                        List<?> content = (List) values.computeIfAbsent(desc.getName(), k -> new ArrayList<>());
                        content.add(resolveFieldValue(stream, desc, unknownFields));
                    } else {
                        values.put(desc.getName(), resolveFieldValue(stream, desc, unknownFields));
                    }
                    Descriptors.OneofDescriptor oneOf = desc.getContainingOneof();
                    if (oneOf != null) {
                        oneOf.getFields().forEach(expected::remove);
                    }
                } else {
                    int fieldWireType = tag & 3;
                    unknownFields.add(new UnknownField(descriptor.getFullName(), fieldNumber, fieldWireType, resolveUnknownField(stream, fieldWireType)));
                }
            }
            if (! expected.isEmpty()) {
                for (Descriptors.FieldDescriptor d: expected) {
                    if (d.isRepeated()) {
                        values.put(d.getName(), List.of());
                    } else {
                        values.put(d.getName(), d.getDefaultValue());
                    }
                }
            }
            return (T) values;
        }
    }

    public Object resolveEnum(Descriptors.FieldDescriptor dfd, int enumKey) {
        return dfd.getEnumType().findValueByNumber(enumKey).getName();
    }

    @SuppressWarnings("unchecked")
    public <T> T resolveFieldValue(CodedInputStream stream, Descriptors.FieldDescriptor dfd, List<BinaryDecoder.UnknownField> unknownFields)
            throws IOException {
        if (fastPathMap.containsKey(dfd.getFullName()) && dfd.getType() != Descriptors.FieldDescriptor.Type.MESSAGE) {
            // If it's a message, fast path will be resolved in readMessageField
            return (T) fastPathMap.get(dfd.getFullName()).resolve(stream, unknownFields);
        } else {
            switch (dfd.getType()) {
            case DOUBLE:
                return (T) Double.valueOf(stream.readDouble());
            case FLOAT:
                return (T) Float.valueOf(stream.readFloat());
            case INT32:
                return (T) Integer.valueOf(stream.readInt32());
            case INT64:
                return (T) Long.valueOf(stream.readInt64());
            case UINT32:
                return (T) Integer.valueOf(stream.readUInt32());
            case UINT64:
                return (T) Long.valueOf(stream.readUInt64());
            case SINT32:
                return (T) Integer.valueOf(stream.readSInt32());
            case SINT64:
                return (T) Long.valueOf(stream.readSInt64());
            case FIXED32:
                return (T) Integer.valueOf(stream.readFixed32());
            case FIXED64:
                return (T) Long.valueOf(stream.readFixed64());
            case SFIXED32:
                return (T) Integer.valueOf(stream.readSFixed32());
            case SFIXED64:
                return (T) Long.valueOf(stream.readSFixed64());
            case BOOL:
                return (T) Boolean.valueOf(stream.readBool());
            case STRING:
                return (T) stream.readString();
            case BYTES:
                return (T) stream.readByteArray();
            case MESSAGE:
                return readMessageField(stream, dfd, unknownFields);
            case ENUM:
                return (T) resolveEnum(dfd, stream.readEnum());
            default:
                throw new IllegalStateException(dfd.getType().name());
            }
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T readMessageField(CodedInputStream stream, Descriptors.FieldDescriptor dfd, List<BinaryDecoder.UnknownField> unknownFields)
            throws IOException {
        T val;
        int len = stream.readRawVarint32();
        int oldLimit = stream.pushLimit(len);
        if (fastPathMap.containsKey(dfd.getFullName())) {
            val = (T) fastPathMap.get(dfd.getFullName()).resolve(stream, unknownFields);
        } else {
            val = parseMessage(stream, dfd.getMessageType(), unknownFields);
        }
        stream.popLimit(oldLimit);
        return val;
    }

    private Object resolveUnknownField(CodedInputStream stream, int wireType) throws IOException {
        switch (wireType) {
        case 0:
            return stream.readRawVarint64();
        case 1:
            return stream.readRawBytes(8);
        case 2:
            int len = stream.readRawVarint32();
            return stream.readRawBytes(len);
        case 3:
        case 4:
            throw new UnsupportedOperationException("group not handled");
        case 5:
            return stream.readRawBytes(4);
        default:
            return null;
        }
    }

    public Descriptors.FieldDescriptor resolve(String name, int tag) {
        int fieldNumber = (tag >> 3);
        if (descriptors.get(name).get(fieldNumber) == null) {
            throw new NullPointerException();
        }
        return descriptors.get(name).get(fieldNumber);
    }

    public Descriptors.Descriptor getMessageDescriptor(String name) {
        return messages.get(name);
    }

    public Descriptors.MethodDescriptor getMethodDescriptor(String name) {
        return methods.get(name);
    }

}
