package loghub.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.Any;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;

import lombok.Data;

public class BinaryDecoder {

   private final Map<String, Map<Integer, Descriptors.GenericDescriptor>> descriptors;
    private final Map<String, FastPathFunction> fastPathMap = new HashMap<>();

    @FunctionalInterface
    public interface FastPathFunction {
        Object resolve(CodedInputStream stream) throws IOException;
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

    private void initFastPath() {
        fastPathMap.put("com.google.protobuf.Any", s -> Any.parseFrom(s.readByteBuffer()));
        fastPathMap.put("com.google.protobuf.Duration", s -> Duration.parseFrom(s.readByteBuffer()));
        fastPathMap.put("com.google.protobuf.Timestamp", s -> Timestamp.parseFrom(s.readByteBuffer()));
    }

    private Map<String, Map<Integer, Descriptors.GenericDescriptor>> analyseProto(InputStream source)
            throws IOException, Descriptors.DescriptorValidationException {
        Map<String, Map<Integer, Descriptors.GenericDescriptor>> current = new HashMap<>();
        for (Descriptors.FileDescriptor fd : resolveProto(source)) {
            for (Descriptors.EnumDescriptor et : fd.getEnumTypes()) {
                scanEnum(et, current);
            }
            for (Descriptors.Descriptor dd : fd.getMessageTypes()) {
                scanDescriptor(dd, current);
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

    private void scanEnum(Descriptors.EnumDescriptor ed,
            Map<String, Map<Integer, Descriptors.GenericDescriptor>> descriptors) {
        for (var ev : ed.getValues()) {
            descriptors.computeIfAbsent(ed.getFullName(), k -> new HashMap<>()).put(ev.getNumber(), ev);
        }
    }

    private void scanDescriptor(Descriptors.Descriptor dd,
            Map<String, Map<Integer, Descriptors.GenericDescriptor>> descriptors) {
        dd.getNestedTypes().forEach(d -> scanDescriptor(d, descriptors));
        for (var et : dd.getEnumTypes()) {
            scanEnum(et, descriptors);
        }
        for (var dfd : dd.getFields()) {
            if (dfd.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                descriptors.computeIfAbsent(dd.getFullName(), k -> new HashMap<>()).put(dfd.getNumber(), dfd);
            } else if (dfd.isMapField()) {
                throw new RuntimeException();
            } else if (dfd.isExtension()) {
                throw new RuntimeException();
            } else {
                descriptors.computeIfAbsent(dd.getFullName(), k -> new HashMap<>()).put(dfd.getNumber(), dfd);
            }
        }
    }

    public void addFastPath(String messageType, FastPathFunction fastPath) {
        if (fastPath == null) {
            fastPathMap.remove(messageType);
        } else {
            fastPathMap.put(messageType, fastPath);
        }
    }

    @Data
    public static class UnknownField {
        private final String message;
        private final int fieldNumber;
        private final int fieldWireType;
        private final Object value;
    }

    public void parseInput(CodedInputStream stream, String messageName, Map<String, Object> values, List<UnknownField> unknownFields) throws IOException {
        Map<Integer, Descriptors.GenericDescriptor> messageMapping = descriptors.get(messageName);
        while (!stream.isAtEnd()) {
            int tag = stream.readTag();
            int fieldNumber = (tag >> 3);
            int fieldWireType = tag & 3;
            Descriptors.GenericDescriptor desc = messageMapping.get(fieldNumber);
            if (desc instanceof Descriptors.FieldDescriptor) {
                resolveValue((Descriptors.FieldDescriptor) desc, stream, values, unknownFields);
            } else {
                unknownFields.add(new UnknownField(messageName, fieldNumber, fieldWireType, resolveUnknownField(stream, fieldWireType)));
            }
        }
    }

    private Object resolveEnum(Descriptors.FieldDescriptor dfd, int enumKey) {
        return dfd.getEnumType().findValueByNumber(enumKey).getName();
    }

    private void resolveValue(Descriptors.FieldDescriptor dfd, CodedInputStream stream, Map<String, Object> values,List<BinaryDecoder.UnknownField> unknownFields)
            throws IOException {
        Object val;
        if (fastPathMap.containsKey(dfd.getFullName()) && dfd.getType() != Descriptors.FieldDescriptor.Type.MESSAGE) {
            val = fastPathMap.get(dfd.getFullName()).resolve(stream);
        } else {
            switch (dfd.getType()) {
            case DOUBLE:
                val = stream.readDouble();
                break;
            case FLOAT:
                val = stream.readFloat();
                break;
            case INT32:
                val = stream.readInt32();
                break;
            case INT64:
                val = stream.readInt64();
                break;
            case UINT32:
                val = stream.readUInt32();
                break;
            case UINT64:
                val = stream.readUInt64();
                break;
            case SINT32:
                val = stream.readSInt32();
                break;
            case SINT64:
                val = stream.readSInt64();
                break;
            case FIXED32:
                val = stream.readFixed32();
                break;
            case FIXED64:
                val = stream.readFixed64();
                break;
            case SFIXED32:
                val = stream.readSFixed32();
                break;
            case SFIXED64:
                val = stream.readSFixed64();
                break;
            case BOOL:
                val = stream.readBool();
                break;
            case STRING:
                val = stream.readString();
                break;
            case BYTES:
                val = stream.readByteArray();
                break;
            case MESSAGE:
                int len = stream.readRawVarint32();
                int oldLimit = stream.pushLimit(len);
                if (fastPathMap.containsKey(dfd.getFullName())) {
                    val = fastPathMap.get(dfd.getFullName()).resolve(stream);
                } else {
                    Map<String, Object> messageValues = new HashMap<>();
                    parseInput(stream, dfd.getMessageType().getFullName(), messageValues, unknownFields);
                    val = messageValues;
                }
                stream.popLimit(oldLimit);
                break;
            case ENUM:
                val = resolveEnum(dfd, stream.readEnum());
                break;
            default:
                throw new IllegalStateException(dfd.getType().name());
            }
        }
        putValue(values, dfd, val);
    }

    private void putValue(Map<String, Object> values, Descriptors.FieldDescriptor dfd, Object value) {
        String name = dfd.getType() == Descriptors.FieldDescriptor.Type.MESSAGE ? dfd.getMessageType().getName() : dfd.getName();
        if (dfd.isRepeated()) {
            @SuppressWarnings("unchecked")
            List<Object> content = (List<Object>) values.computeIfAbsent(name, k -> new ArrayList<>());
            content.add(value);
        } else {
            values.put(name, value);
        }
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
            return stream;
        case 4:
            return stream;
        case 5:
            return stream.readRawBytes(4);
        default:
            return null;
        }
    }

}
