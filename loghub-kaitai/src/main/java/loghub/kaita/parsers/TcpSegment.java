package loghub.kaita.parsers;
// This is a generated file! Please edit source .ksy file and use kaitai-struct-compiler to rebuild

import io.kaitai.struct.ByteBufferKaitaiStream;
import io.kaitai.struct.KaitaiStruct;
import io.kaitai.struct.KaitaiStream;
import java.io.IOException;


/**
 * TCP is one of the core Internet protocols on transport layer (AKA
 * OSI layer 4), providing stateful connections with error checking,
 * guarantees of delivery, order of segments and avoidance of duplicate
 * delivery.
 */
public class TcpSegment extends KaitaiStruct {
    public static TcpSegment fromFile(String fileName) throws IOException {
        return new TcpSegment(new ByteBufferKaitaiStream(fileName));
    }

    public TcpSegment(KaitaiStream _io) {
        this(_io, null, null);
    }

    public TcpSegment(KaitaiStream _io, KaitaiStruct _parent) {
        this(_io, _parent, null);
    }

    public TcpSegment(KaitaiStream _io, KaitaiStruct _parent, TcpSegment _root) {
        super(_io);
        this._parent = _parent;
        this._root = _root == null ? this : _root;
        _read();
    }
    private void _read() {
        this.srcPort = this._io.readU2be();
        this.dstPort = this._io.readU2be();
        this.seqNum = this._io.readU4be();
        this.ackNum = this._io.readU4be();
        this.dataOffset = this._io.readBitsIntBe(4);
        this.reserved = this._io.readBitsIntBe(4);
        this._io.alignToByte();
        this.flags = new Flags(this._io, this, _root);
        this.windowSize = this._io.readU2be();
        this.checksum = this._io.readU2be();
        this.urgentPointer = this._io.readU2be();
        if (((dataOffset() * 4) - 20) != 0) {
            this.options = this._io.readBytes(((dataOffset() * 4) - 20));
        }
        this.body = this._io.readBytesFull();
    }

    /**
     * TCP header flags as defined "TCP Header Flags" registry.
     */
    public static class Flags extends KaitaiStruct {
        public static Flags fromFile(String fileName) throws IOException {
            return new Flags(new ByteBufferKaitaiStream(fileName));
        }

        public Flags(KaitaiStream _io) {
            this(_io, null, null);
        }

        public Flags(KaitaiStream _io, TcpSegment _parent) {
            this(_io, _parent, null);
        }

        public Flags(KaitaiStream _io, TcpSegment _parent, TcpSegment _root) {
            super(_io);
            this._parent = _parent;
            this._root = _root;
            _read();
        }
        private void _read() {
            this.cwr = this._io.readBitsIntBe(1) != 0;
            this.ece = this._io.readBitsIntBe(1) != 0;
            this.urg = this._io.readBitsIntBe(1) != 0;
            this.ack = this._io.readBitsIntBe(1) != 0;
            this.psh = this._io.readBitsIntBe(1) != 0;
            this.rst = this._io.readBitsIntBe(1) != 0;
            this.syn = this._io.readBitsIntBe(1) != 0;
            this.fin = this._io.readBitsIntBe(1) != 0;
        }
        private boolean cwr;
        private boolean ece;
        private boolean urg;
        private boolean ack;
        private boolean psh;
        private boolean rst;
        private boolean syn;
        private boolean fin;
        private TcpSegment _root;
        private TcpSegment _parent;

        /**
         * Congestion Window Reduced
         */
        public boolean cwr() { return cwr; }

        /**
         * ECN-Echo
         */
        public boolean ece() { return ece; }

        /**
         * Urgent pointer field is significant
         */
        public boolean urg() { return urg; }

        /**
         * Acknowledgment field is significant
         */
        public boolean ack() { return ack; }

        /**
         * Push function
         */
        public boolean psh() { return psh; }

        /**
         * Reset the connection
         */
        public boolean rst() { return rst; }

        /**
         * Synchronize sequence numbers
         */
        public boolean syn() { return syn; }

        /**
         * No more data from sender
         */
        public boolean fin() { return fin; }
        public TcpSegment _root() { return _root; }
        public TcpSegment _parent() { return _parent; }
    }
    private int srcPort;
    private int dstPort;
    private long seqNum;
    private long ackNum;
    private long dataOffset;
    private long reserved;
    private Flags flags;
    private int windowSize;
    private int checksum;
    private int urgentPointer;
    private byte[] options;
    private byte[] body;
    private TcpSegment _root;
    private KaitaiStruct _parent;

    /**
     * Source port
     */
    public int srcPort() { return srcPort; }

    /**
     * Destination port
     */
    public int dstPort() { return dstPort; }

    /**
     * Sequence number
     */
    public long seqNum() { return seqNum; }

    /**
     * Acknowledgment number
     */
    public long ackNum() { return ackNum; }

    /**
     * Data offset (in 32-bit words from the beginning of this type, normally 32 or can be extended if there are any TCP options or padding is present)
     */
    public long dataOffset() { return dataOffset; }
    public long reserved() { return reserved; }
    public Flags flags() { return flags; }
    public int windowSize() { return windowSize; }
    public int checksum() { return checksum; }
    public int urgentPointer() { return urgentPointer; }
    public byte[] options() { return options; }
    public byte[] body() { return body; }
    public TcpSegment _root() { return _root; }
    public KaitaiStruct _parent() { return _parent; }
}
