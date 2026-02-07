package loghub.kaita.parsers;// This is a generated file! Please edit source .ksy file and use kaitai-struct-compiler to rebuild

import java.io.IOException;

import io.kaitai.struct.ByteBufferKaitaiStream;
import io.kaitai.struct.KaitaiStream;
import io.kaitai.struct.KaitaiStruct;


/**
 * UDP is a simple stateless transport layer (AKA OSI layer 4)
 * protocol, one of the core Internet protocols. It provides source and
 * destination ports, basic checksumming, but provides not guarantees
 * of delivery, order of packets, or duplicate delivery.
 */
public class UdpDatagram extends KaitaiStruct {
    public static UdpDatagram fromFile(String fileName) throws IOException {
        return new UdpDatagram(new ByteBufferKaitaiStream(fileName));
    }

    public UdpDatagram(KaitaiStream _io) {
        this(_io, null, null);
    }

    public UdpDatagram(KaitaiStream _io, KaitaiStruct _parent) {
        this(_io, _parent, null);
    }

    public UdpDatagram(KaitaiStream _io, KaitaiStruct _parent, UdpDatagram _root) {
        super(_io);
        this._parent = _parent;
        this._root = _root == null ? this : _root;
        _read();
    }
    private void _read() {
        this.srcPort = this._io.readU2be();
        this.dstPort = this._io.readU2be();
        this.length = this._io.readU2be();
        this.checksum = this._io.readU2be();
        this.body = this._io.readBytes((length() - 8));
    }
    private int srcPort;
    private int dstPort;
    private int length;
    private int checksum;
    private byte[] body;
    private UdpDatagram _root;
    private KaitaiStruct _parent;
    public int srcPort() { return srcPort; }
    public int dstPort() { return dstPort; }
    public int length() { return length; }
    public int checksum() { return checksum; }
    public byte[] body() { return body; }
    public UdpDatagram _root() { return _root; }
    public KaitaiStruct _parent() { return _parent; }
}
