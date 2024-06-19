package loghub.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;

public class BinaryDecoder {

    private final Map<String, Map<Integer, Descriptors.GenericDescriptor>> descriptors;

    BinaryDecoder(InputStream source) throws Descriptors.DescriptorValidationException, IOException {
        descriptors = analyseProto(source);
    }

    BinaryDecoder(Path source) throws Descriptors.DescriptorValidationException, IOException {
        try (InputStream is = Files.newInputStream(source)) {
            descriptors = analyseProto(is);
        }
    }

    public Map<String, Map<Integer, Descriptors.GenericDescriptor>> analyseProto(InputStream source) throws IOException, Descriptors.DescriptorValidationException {
        Map<String, Map<Integer, Descriptors.GenericDescriptor>> current = new HashMap<>();
        for (Descriptors.FileDescriptor fd: resolveProto(source)) {
            for (var et: fd.getEnumTypes()) {
                scanEnum(et, current);
            }
            for (Descriptors.Descriptor dd: fd.getMessageTypes()) {
                scanDescriptor(dd, current);
            }
        }
        return current;
    }

    private List<Descriptors.FileDescriptor> resolveProto(InputStream source) throws IOException, Descriptors.DescriptorValidationException {
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

    private void scanEnum(Descriptors.EnumDescriptor ed, Map<String, Map<Integer, Descriptors.GenericDescriptor>> descriptors) {
        for (var ev: ed.getValues()) {
            descriptors.computeIfAbsent(ed.getFullName(), k -> new HashMap<>()).put(ev.getNumber(), ev);
        }
    }

    private void scanDescriptor(Descriptors.Descriptor dd, Map<String, Map<Integer, Descriptors.GenericDescriptor>> descriptors) {
        dd.getNestedTypes().forEach(d -> scanDescriptor(d, descriptors));
        for (var et: dd.getEnumTypes()) {
            scanEnum(et, descriptors);
        }
        for (var dfd: dd.getExtensions()) {
            throw new RuntimeException();
        }
        for (var dfd: dd.getFields()) {
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

    public void parseInput(ByteBuffer buffer, String messageName, Map<String, Object> values) throws IOException {
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        Map<Integer, Descriptors.GenericDescriptor> messageMapping = descriptors.get(messageName);
        while (buffer.hasRemaining()) {
            long tag = readRawVarint32(buffer);
            int fieldNumber = (int) (tag >> 3);
            byte wireType = (byte) (tag & 7);
            Descriptors.GenericDescriptor desc = messageMapping.get(fieldNumber);
            if (wireType == 0 && desc instanceof Descriptors.FieldDescriptor) {
                readWireVarInt((Descriptors.FieldDescriptor)desc, buffer, values);
            } else if (wireType == 0 && desc instanceof Descriptors.EnumValueDescriptor) {
                readWireVarInt((Descriptors.EnumValueDescriptor)desc, buffer, values);
            } else if (wireType == 1 && desc instanceof Descriptors.FieldDescriptor) {
                readWireI64((Descriptors.FieldDescriptor) desc, buffer, values);
            } else if (wireType == 2 && desc instanceof Descriptors.FieldDescriptor) {
                readWireLen((Descriptors.FieldDescriptor) desc, buffer, values);
            } else if (wireType == 8 && desc instanceof Descriptors.FieldDescriptor) {
                readWireI32((Descriptors.FieldDescriptor) desc, buffer, values);
            } else {
                throw new RuntimeException("" + wireType);
            }
        }
    }

    /**
     * Read int32, int64, uint32, uint64, sint32, sint64, bool, enum
     * @param dfd
     * @param buffer
     * @param values
     * @throws IOException
     */
    private void readWireVarInt(Descriptors.FieldDescriptor dfd, ByteBuffer buffer,Map<String, Object> values)
            throws IOException {
        switch (dfd.getType()) {
        case INT64:
            putValue(values, dfd, readRawVarint64(buffer));
        }
    }

    /**
     * Read int32, int64, uint32, uint64, sint32, sint64, bool, enum
     * @param devd
     * @param buffer
     * @param values
     * @throws IOException
     */
    private void readWireVarInt(Descriptors.EnumValueDescriptor devd, ByteBuffer buffer,Map<String, Object> values)
            throws IOException {

    }

    /**
     * Read fixed64, sfixed64, double
     * @param dfd
     * @param buffer
     * @param values
     * @throws IOException
     */
    private void readWireI64(Descriptors.FieldDescriptor dfd, ByteBuffer buffer,Map<String, Object> values)
            throws IOException {
        switch (dfd.getType()) {
        case FIXED64:
        case SFIXED64:
            break;
        case DOUBLE:
            putValue(values, dfd, buffer.getDouble());
            break;
        }
    }

    /**
     * Read string, bytes, embedded messages, packed repeated fields
     * @param dfd
     * @param buffer
     * @param values
     * @throws IOException
     */
    private void readWireLen(Descriptors.FieldDescriptor dfd, ByteBuffer buffer,Map<String, Object> values)
            throws IOException {
        int len = readRawVarint32(buffer);
        ByteBuffer newBuffer = buffer.slice().limit(len);
        buffer.position(buffer.position() + len);
        switch (dfd.getType()) {
        case MESSAGE:
            Map<String, Object> messageValues = new HashMap<>();
            parseInput(newBuffer, dfd.getMessageType().getFullName(), messageValues);
            putValue(values, dfd, messageValues);
            break;
        case STRING:
            byte[] bytes = new byte[(int) len];
            newBuffer.get(bytes);
            putValue(values, dfd, new String(bytes, StandardCharsets.UTF_8));
            break;
        case BYTES:
        }
    }

    /**
     * Read fixed32, sfixed32, float
     * @param dfd
     * @param buffer
     * @param values
     * @throws IOException
     */
    private void readWireI32(Descriptors.FieldDescriptor dfd, ByteBuffer buffer,Map<String, Object> values)
            throws IOException {
        switch (dfd.getType()) {
        case FIXED32:
        case SFIXED32:
            break;
        case FLOAT:
            putValue(values, dfd, buffer.getFloat());
        }
    }


    private void putValue(Map<String, Object> values, Descriptors.FieldDescriptor dfd, Object value) {
        String name = dfd.getType() == Descriptors.FieldDescriptor.Type.MESSAGE ? dfd.getMessageType().getName() : dfd.getName();
        if (dfd.isRepeated()) {
            List content = (List) values.computeIfAbsent(name, k -> new ArrayList<>());
            content.add(value);
        } else {
            values.put(name, value);
        }
    }

    /*public long readVarint(ByteBuffer buffer) {
        CodedInputStream cis;
        long varInt = 0;
        int lastByte;
        do {
            lastByte = buffer.get();
            if (lastByte < 0) {
                varInt = varInt << 7;
                varInt += (lastByte + 128) & 127;
            } else {
                varInt += lastByte;
            }
        } while(lastByte < 0);
        return varInt;
    }*/

    public int readRawVarint32(ByteBuffer buffer) throws IOException {
        // See implementation notes for readRawVarint64
        int x;
        if ((x = buffer.get()) >= 0) {
            return x;
        } else if ((x ^= (buffer.get() << 7)) < 0) {
            x ^= (~0 << 7);
        } else if ((x ^= (buffer.get() << 14)) >= 0) {
            x ^= (~0 << 7) ^ (~0 << 14);
        } else if ((x ^= (buffer.get() << 21)) < 0) {
            x ^= (~0 << 7) ^ (~0 << 14) ^ (~0 << 21);
        } else {
            int y = buffer.get();
            x ^= y << 28;
            x ^= (~0 << 7) ^ (~0 << 14) ^ (~0 << 21) ^ (~0 << 28);
            if (y < 0
                        && buffer.get() < 0
                        && buffer.get() < 0
                        && buffer.get() < 0
                        && buffer.get() < 0
                        && buffer.get() < 0) {
                throw new InvalidProtocolBufferException("CodedInputStream encountered a malformed varint");
            }
        }
        return x;
    }

    public long readRawVarint64(ByteBuffer buffer) throws IOException {
        // Implementation notes:
        //
        // Optimized for one-byte values, expected to be common.
        // The particular code below was selected from various candidates
        // empirically, by winning VarintBenchmark.
        //
        // Sign extension of (signed) Java bytes is usually a nuisance, but
        // we exploit it here to more easily obtain the sign of bytes read.
        // Instead of cleaning up the sign extension bits by masking eagerly,
        // we delay until we find the final (positive) byte, when we clear all
        // accumulated bits with one xor.  We depend on javac to constant fold.
        long x;
        int y;
        if ((y = buffer.get()) >= 0) {
            return y;
        } else if ((y ^= (buffer.get() << 7)) < 0) {
            x = y ^ (~0 << 7);
        } else if ((y ^= (buffer.get() << 14)) >= 0) {
            x = y ^ ((~0 << 7) ^ (~0 << 14));
        } else if ((y ^= (buffer.get() << 21)) < 0) {
            x = y ^ ((~0 << 7) ^ (~0 << 14) ^ (~0 << 21));
        } else if ((x = y ^ ((long) buffer.get() << 28)) >= 0L) {
            x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28);
        } else if ((x ^= ((long) buffer.get() << 35)) < 0L) {
            x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35);
        } else if ((x ^= ((long) buffer.get() << 42)) >= 0L) {
            x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35) ^ (~0L << 42);
        } else if ((x ^= ((long) buffer.get() << 49)) < 0L) {
            x ^=
                    (~0L << 7)
                            ^ (~0L << 14)
                            ^ (~0L << 21)
                            ^ (~0L << 28)
                            ^ (~0L << 35)
                            ^ (~0L << 42)
                            ^ (~0L << 49);
        } else {
            x ^= ((long) buffer.get() << 56);
            x ^=
                    (~0L << 7)
                            ^ (~0L << 14)
                            ^ (~0L << 21)
                            ^ (~0L << 28)
                            ^ (~0L << 35)
                            ^ (~0L << 42)
                            ^ (~0L << 49)
                            ^ (~0L << 56);
            if (x < 0L) {
                if (buffer.get() < 0L) {
                    throw new InvalidProtocolBufferException("CodedInputStream encountered a malformed varint");
                }
            }
        }
        return x;
    }

    long readRawVarint64SlowPath(ByteBuffer buffer) throws IOException {
        long result = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            byte b = buffer.get();
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new InvalidProtocolBufferException("CodedInputStream encountered a malformed varint");
    }


}
