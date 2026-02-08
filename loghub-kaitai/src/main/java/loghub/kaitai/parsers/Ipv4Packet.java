// This is a generated file! Please edit source .ksy file and use kaitai-struct-compiler to rebuild

package loghub.kaitai.parsers;

import io.kaitai.struct.ByteBufferKaitaiStream;
import io.kaitai.struct.KaitaiStruct;
import io.kaitai.struct.KaitaiStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class Ipv4Packet extends KaitaiStruct {
    public static Ipv4Packet fromFile(String fileName) throws IOException {
        return new Ipv4Packet(new ByteBufferKaitaiStream(fileName));
    }

    public Ipv4Packet(KaitaiStream _io) {
        this(_io, null, null);
    }

    public Ipv4Packet(KaitaiStream _io, KaitaiStruct _parent) {
        this(_io, _parent, null);
    }

    public Ipv4Packet(KaitaiStream _io, KaitaiStruct _parent, Ipv4Packet _root) {
        super(_io);
        this._parent = _parent;
        this._root = _root == null ? this : _root;
        _read();
    }
    private void _read() {
        this.b1 = this._io.readU1();
        this.b2 = this._io.readU1();
        this.totalLength = this._io.readU2be();
        this.identification = this._io.readU2be();
        this.b67 = this._io.readU2be();
        this.ttl = this._io.readU1();
        this.protocol = this._io.readU1();
        this.headerChecksum = this._io.readU2be();
        try {
            this.srcIpAddr = InetAddress.getByAddress(this._io.readBytes(4));
            this.dstIpAddr = InetAddress.getByAddress(this._io.readBytes(4));
        } catch (UnknownHostException e) {
            // unreachable
        }
        KaitaiStream _io_options = this._io.substream(ihlBytes() - 20);
        this.options = new Ipv4Options(_io_options, this, _root);
        KaitaiStream _io_body = this._io.substream(totalLength() - ihlBytes());
        this.body = new ProtocolBody(_io_body, this, protocol());
    }

    public void _fetchInstances() {
        this.options._fetchInstances();
        this.body._fetchInstances();
    }
    public static class Ipv4Option extends KaitaiStruct {
        public static Ipv4Option fromFile(String fileName) throws IOException {
            return new Ipv4Option(new ByteBufferKaitaiStream(fileName));
        }

        public Ipv4Option(KaitaiStream _io) {
            this(_io, null, null);
        }

        public Ipv4Option(KaitaiStream _io, Ipv4Packet.Ipv4Options _parent) {
            this(_io, _parent, null);
        }

        public Ipv4Option(KaitaiStream _io, Ipv4Packet.Ipv4Options _parent, Ipv4Packet _root) {
            super(_io);
            this._parent = _parent;
            this._root = _root;
            _read();
        }
        private void _read() {
            this.b1 = this._io.readU1();
            this.len = this._io.readU1();
            this.body = this._io.readBytes((len() > 2 ? len() - 2 : 0));
        }

        public void _fetchInstances() {
        }
        private Integer copy;
        public Integer copy() {
            if (this.copy != null)
                return this.copy;
            this.copy = ((Number) ((b1() & 128) >> 7)).intValue();
            return this.copy;
        }
        private Integer number;
        public Integer number() {
            if (this.number != null)
                return this.number;
            this.number = ((Number) (b1() & 31)).intValue();
            return this.number;
        }
        private Integer optClass;
        public Integer optClass() {
            if (this.optClass != null)
                return this.optClass;
            this.optClass = ((Number) ((b1() & 96) >> 5)).intValue();
            return this.optClass;
        }
        private int b1;
        private int len;
        private byte[] body;
        private Ipv4Packet _root;
        private Ipv4Packet.Ipv4Options _parent;
        public int b1() { return b1; }
        public int len() { return len; }
        public byte[] body() { return body; }
        public Ipv4Packet _root() { return _root; }
        public Ipv4Packet.Ipv4Options _parent() { return _parent; }
    }
    public static class Ipv4Options extends KaitaiStruct {
        public static Ipv4Options fromFile(String fileName) throws IOException {
            return new Ipv4Options(new ByteBufferKaitaiStream(fileName));
        }

        public Ipv4Options(KaitaiStream _io) {
            this(_io, null, null);
        }

        public Ipv4Options(KaitaiStream _io, Ipv4Packet _parent) {
            this(_io, _parent, null);
        }

        public Ipv4Options(KaitaiStream _io, Ipv4Packet _parent, Ipv4Packet _root) {
            super(_io);
            this._parent = _parent;
            this._root = _root;
            _read();
        }
        private void _read() {
            this.entries = new ArrayList<Ipv4Option>();
            {
                int i = 0;
                while (!this._io.isEof()) {
                    this.entries.add(new Ipv4Option(this._io, this, _root));
                    i++;
                }
            }
        }

        public void _fetchInstances() {
            for (int i = 0; i < this.entries.size(); i++) {
                this.entries.get(((Number) (i)).intValue())._fetchInstances();
            }
        }
        private List<Ipv4Option> entries;
        private Ipv4Packet _root;
        private Ipv4Packet _parent;
        public List<Ipv4Option> entries() { return entries; }
        public Ipv4Packet _root() { return _root; }
        public Ipv4Packet _parent() { return _parent; }
    }
    private Integer ihl;
    public Integer ihl() {
        if (this.ihl != null)
            return this.ihl;
        this.ihl = ((Number) (b1() & 15)).intValue();
        return this.ihl;
    }
    private Integer ihlBytes;
    public Integer ihlBytes() {
        if (this.ihlBytes != null)
            return this.ihlBytes;
        this.ihlBytes = ((Number) (ihl() * 4)).intValue();
        return this.ihlBytes;
    }
    private Integer version;
    public Integer version() {
        if (this.version != null)
            return this.version;
        this.version = ((Number) ((b1() & 240) >> 4)).intValue();
        return this.version;
    }
    private int b1;
    private int b2;
    private int totalLength;
    private int identification;
    private int b67;
    private int ttl;
    private int protocol;
    private int headerChecksum;
    private InetAddress srcIpAddr;
    private InetAddress dstIpAddr;
    private Ipv4Options options;
    private ProtocolBody body;
    private Ipv4Packet _root;
    private KaitaiStruct _parent;
    public int b1() { return b1; }
    public int b2() { return b2; }
    public int totalLength() { return totalLength; }
    public int identification() { return identification; }
    public int b67() { return b67; }
    public int ttl() { return ttl; }
    public int protocol() { return protocol; }
    public int headerChecksum() { return headerChecksum; }
    public InetAddress srcIpAddr() { return srcIpAddr; }
    public InetAddress dstIpAddr() { return dstIpAddr; }
    public Ipv4Options options() { return options; }
    public ProtocolBody body() { return body; }
    public Ipv4Packet _root() { return _root; }
    public KaitaiStruct _parent() { return _parent; }
}
