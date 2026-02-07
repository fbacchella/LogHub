package loghub.kaita.parsers;
// This is a generated file! Please edit source .ksy file and use kaitai-struct-compiler to rebuild

import io.kaitai.struct.ByteBufferKaitaiStream;
import io.kaitai.struct.KaitaiStruct;
import io.kaitai.struct.KaitaiStream;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;

public class IcmpPacket extends KaitaiStruct {
    public static IcmpPacket fromFile(String fileName) throws IOException {
        return new IcmpPacket(new ByteBufferKaitaiStream(fileName));
    }

    public enum IcmpTypeEnum {
        ECHO_REPLY(0),
        DESTINATION_UNREACHABLE(3),
        SOURCE_QUENCH(4),
        REDIRECT(5),
        ECHO(8),
        TIME_EXCEEDED(11);

        private final long id;
        IcmpTypeEnum(long id) { this.id = id; }
        public long id() { return id; }
        private static final Map<Long, IcmpTypeEnum> byId = new HashMap<Long, IcmpTypeEnum>(6);
        static {
            for (IcmpTypeEnum e : IcmpTypeEnum.values())
                byId.put(e.id(), e);
        }
        public static IcmpTypeEnum byId(long id) { return byId.get(id); }
    }

    public IcmpPacket(KaitaiStream _io) {
        this(_io, null, null);
    }

    public IcmpPacket(KaitaiStream _io, KaitaiStruct _parent) {
        this(_io, _parent, null);
    }

    public IcmpPacket(KaitaiStream _io, KaitaiStruct _parent, IcmpPacket _root) {
        super(_io);
        this._parent = _parent;
        this._root = _root == null ? this : _root;
        _read();
    }
    private void _read() {
        this.icmpType = IcmpTypeEnum.byId(this._io.readU1());
        if (icmpType() == IcmpTypeEnum.DESTINATION_UNREACHABLE) {
            this.destinationUnreachable = new DestinationUnreachableMsg(this._io, this, _root);
        }
        if (icmpType() == IcmpTypeEnum.TIME_EXCEEDED) {
            this.timeExceeded = new TimeExceededMsg(this._io, this, _root);
        }
        if ( ((icmpType() == IcmpTypeEnum.ECHO) || (icmpType() == IcmpTypeEnum.ECHO_REPLY)) ) {
            this.echo = new EchoMsg(this._io, this, _root);
        }
    }

    public void _fetchInstances() {
        if (icmpType() == IcmpTypeEnum.DESTINATION_UNREACHABLE) {
            this.destinationUnreachable._fetchInstances();
        }
        if (icmpType() == IcmpTypeEnum.TIME_EXCEEDED) {
            this.timeExceeded._fetchInstances();
        }
        if ( ((icmpType() == IcmpTypeEnum.ECHO) || (icmpType() == IcmpTypeEnum.ECHO_REPLY)) ) {
            this.echo._fetchInstances();
        }
    }
    public static class DestinationUnreachableMsg extends KaitaiStruct {
        public static DestinationUnreachableMsg fromFile(String fileName) throws IOException {
            return new DestinationUnreachableMsg(new ByteBufferKaitaiStream(fileName));
        }

        public enum DestinationUnreachableCode {
            NET_UNREACHABLE(0),
            HOST_UNREACHABLE(1),
            PROTOCOL_UNREACHABLE(2),
            PORT_UNREACHABLE(3),
            FRAGMENTATION_NEEDED_AND_DF_SET(4),
            SOURCE_ROUTE_FAILED(5),
            DST_NET_UNKNOWN(6),
            SDT_HOST_UNKNOWN(7),
            SRC_ISOLATED(8),
            NET_PROHIBITED_BY_ADMIN(9),
            HOST_PROHIBITED_BY_ADMIN(10),
            NET_UNREACHABLE_FOR_TOS(11),
            HOST_UNREACHABLE_FOR_TOS(12),
            COMMUNICATION_PROHIBITED_BY_ADMIN(13),
            HOST_PRECEDENCE_VIOLATION(14),
            PRECEDENCE_CUTTOFF_IN_EFFECT(15);

            private final long id;
            DestinationUnreachableCode(long id) { this.id = id; }
            public long id() { return id; }
            private static final Map<Long, DestinationUnreachableCode> byId = new HashMap<Long, DestinationUnreachableCode>(16);
            static {
                for (DestinationUnreachableCode e : DestinationUnreachableCode.values())
                    byId.put(e.id(), e);
            }
            public static DestinationUnreachableCode byId(long id) { return byId.get(id); }
        }

        public DestinationUnreachableMsg(KaitaiStream _io) {
            this(_io, null, null);
        }

        public DestinationUnreachableMsg(KaitaiStream _io, IcmpPacket _parent) {
            this(_io, _parent, null);
        }

        public DestinationUnreachableMsg(KaitaiStream _io, IcmpPacket _parent, IcmpPacket _root) {
            super(_io);
            this._parent = _parent;
            this._root = _root;
            _read();
        }
        private void _read() {
            this.code = DestinationUnreachableCode.byId(this._io.readU1());
            this.checksum = this._io.readU2be();
        }

        public void _fetchInstances() {
        }
        private DestinationUnreachableCode code;
        private int checksum;
        private IcmpPacket _root;
        private IcmpPacket _parent;
        public DestinationUnreachableCode code() { return code; }
        public int checksum() { return checksum; }
        public IcmpPacket _root() { return _root; }
        public IcmpPacket _parent() { return _parent; }
    }
    public static class EchoMsg extends KaitaiStruct {
        public static EchoMsg fromFile(String fileName) throws IOException {
            return new EchoMsg(new ByteBufferKaitaiStream(fileName));
        }

        public EchoMsg(KaitaiStream _io) {
            this(_io, null, null);
        }

        public EchoMsg(KaitaiStream _io, IcmpPacket _parent) {
            this(_io, _parent, null);
        }

        public EchoMsg(KaitaiStream _io, IcmpPacket _parent, IcmpPacket _root) {
            super(_io);
            this._parent = _parent;
            this._root = _root;
            _read();
        }
        private void _read() {
            this.code = this._io.readBytes(1);
            if (!(Arrays.equals(this.code, new byte[] { 0 }))) {
                throw new KaitaiStream.ValidationNotEqualError(new byte[] { 0 }, this.code, this._io, "/types/echo_msg/seq/0");
            }
            this.checksum = this._io.readU2be();
            this.identifier = this._io.readU2be();
            this.seqNum = this._io.readU2be();
            this.data = this._io.readBytesFull();
        }

        public void _fetchInstances() {
        }
        private byte[] code;
        private int checksum;
        private int identifier;
        private int seqNum;
        private byte[] data;
        private IcmpPacket _root;
        private IcmpPacket _parent;
        public byte[] code() { return code; }
        public int checksum() { return checksum; }
        public int identifier() { return identifier; }
        public int seqNum() { return seqNum; }
        public byte[] data() { return data; }
        public IcmpPacket _root() { return _root; }
        public IcmpPacket _parent() { return _parent; }
    }
    public static class TimeExceededMsg extends KaitaiStruct {
        public static TimeExceededMsg fromFile(String fileName) throws IOException {
            return new TimeExceededMsg(new ByteBufferKaitaiStream(fileName));
        }

        public enum TimeExceededCode {
            TIME_TO_LIVE_EXCEEDED_IN_TRANSIT(0),
            FRAGMENT_REASSEMBLY_TIME_EXCEEDED(1);

            private final long id;
            TimeExceededCode(long id) { this.id = id; }
            public long id() { return id; }
            private static final Map<Long, TimeExceededCode> byId = new HashMap<Long, TimeExceededCode>(2);
            static {
                for (TimeExceededCode e : TimeExceededCode.values())
                    byId.put(e.id(), e);
            }
            public static TimeExceededCode byId(long id) { return byId.get(id); }
        }

        public TimeExceededMsg(KaitaiStream _io) {
            this(_io, null, null);
        }

        public TimeExceededMsg(KaitaiStream _io, IcmpPacket _parent) {
            this(_io, _parent, null);
        }

        public TimeExceededMsg(KaitaiStream _io, IcmpPacket _parent, IcmpPacket _root) {
            super(_io);
            this._parent = _parent;
            this._root = _root;
            _read();
        }
        private void _read() {
            this.code = TimeExceededCode.byId(this._io.readU1());
            this.checksum = this._io.readU2be();
        }

        public void _fetchInstances() {
        }
        private TimeExceededCode code;
        private int checksum;
        private IcmpPacket _root;
        private IcmpPacket _parent;
        public TimeExceededCode code() { return code; }
        public int checksum() { return checksum; }
        public IcmpPacket _root() { return _root; }
        public IcmpPacket _parent() { return _parent; }
    }
    private IcmpTypeEnum icmpType;
    private DestinationUnreachableMsg destinationUnreachable;
    private TimeExceededMsg timeExceeded;
    private EchoMsg echo;
    private IcmpPacket _root;
    private KaitaiStruct _parent;
    public IcmpTypeEnum icmpType() { return icmpType; }
    public DestinationUnreachableMsg destinationUnreachable() { return destinationUnreachable; }
    public TimeExceededMsg timeExceeded() { return timeExceeded; }
    public EchoMsg echo() { return echo; }
    public IcmpPacket _root() { return _root; }
    public KaitaiStruct _parent() { return _parent; }
}
