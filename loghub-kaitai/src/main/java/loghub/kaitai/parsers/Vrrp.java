// This is a generated file! Please edit source .ksy file and use kaitai-struct-compiler to rebuild

package loghub.kaitai.parsers;

import io.kaitai.struct.ByteBufferKaitaiStream;
import io.kaitai.struct.KaitaiStruct;
import io.kaitai.struct.KaitaiStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Supports both VRRP version 2 (RFC 3768) and version 3 (RFC 5798).
 * Version 2 supports only IPv4, version 3 supports both IPv4 and IPv6.
 */
public class Vrrp extends KaitaiStruct {

    public Vrrp(KaitaiStream _io, int ipVersion) {
        this(_io, null, null, ipVersion);
    }

    public Vrrp(KaitaiStream _io, KaitaiStruct _parent, int ipVersion) {
        this(_io, _parent, null, ipVersion);
    }

    public Vrrp(KaitaiStream _io, KaitaiStruct _parent, Vrrp _root, int ipVersion) {
        super(_io);
        this._parent = _parent;
        this._root = _root == null ? this : _root;
        this.ipVersion = ipVersion;
        _read();
    }
    private void _read() {
        this.versionType = this._io.readU1();
        this.vrid = this._io.readU1();
        this.priority = this._io.readU1();
        this.numIpAddresses = this._io.readU1();
        this.authTypeOrMaxAdvertIntHigh = this._io.readU1();
        this.advertIntOrMaxAdvertIntLow = this._io.readU1();
        this.checksum = this._io.readU2be();
        if ( ((version() == 2) || ( ((version() == 3) && (ipVersion() == 4)) )) ) {
            this.ipAddresses = new ArrayList<IpAddress>();
            for (int i = 0; i < numIpAddresses(); i++) {
                this.ipAddresses.add(new IpAddress(this._io, this, _root));
            }
        }
        if ( ((version() == 2) && (authType() != 0)) ) {
            this.authenticationData = this._io.readBytes(8);
        }
    }

    public void _fetchInstances() {
        if ( ((version() == 2) || ( ((version() == 3) && (ipVersion() == 4)) )) ) {
            for (int i = 0; i < this.ipAddresses.size(); i++) {
                this.ipAddresses.get(((Number) (i)).intValue())._fetchInstances();
            }
        }
        if ( ((version() == 2) && (authType() != 0)) ) {
        }
    }
    public static class IpAddress extends KaitaiStruct {
        public static IpAddress fromFile(String fileName) throws IOException {
            return new IpAddress(new ByteBufferKaitaiStream(fileName));
        }

        public IpAddress(KaitaiStream _io) {
            this(_io, null, null);
        }

        public IpAddress(KaitaiStream _io, Vrrp _parent) {
            this(_io, _parent, null);
        }

        public IpAddress(KaitaiStream _io, Vrrp _parent, Vrrp _root) {
            super(_io);
            this._parent = _parent;
            this._root = _root;
            _read();
        }
        private void _read() {
            this.address = this._io.readBytes(_parent().ipAddrLen());
        }

        public void _fetchInstances() {
        }
        private byte[] address;
        private Vrrp _root;
        private Vrrp _parent;

        /**
         * IPv4 (4 bytes) or IPv6 (16 bytes) address
         */
        public byte[] address() { return address; }
        public Vrrp _root() { return _root; }
        public Vrrp _parent() { return _parent; }
    }
    private Integer advertIntSeconds;

    /**
     * Advertisement interval in seconds (VRRPv2)
     */
    public Integer advertIntSeconds() {
        if (this.advertIntSeconds != null)
            return this.advertIntSeconds;
        if (version() == 2) {
            this.advertIntSeconds = ((Number) (advertIntOrMaxAdvertIntLow())).intValue();
        }
        return this.advertIntSeconds;
    }
    private Integer authType;

    /**
     * Authentication type (VRRPv2 only)
     */
    public Integer authType() {
        if (this.authType != null)
            return this.authType;
        if (version() == 2) {
            this.authType = ((Number) (authTypeOrMaxAdvertIntHigh())).intValue();
        }
        return this.authType;
    }
    private Byte ipAddrLen;

    /**
     * Length of each IP address in bytes
     * VRRPv2: always 4 (IPv4 only)
     * VRRPv3: 4 for IPv4, 16 for IPv6
     */
    public Byte ipAddrLen() {
        if (this.ipAddrLen != null)
            return this.ipAddrLen;
        this.ipAddrLen = ((Number) (( ((version() == 3) && (ipVersion() == 6))  ? 16 : 4))).byteValue();
        return this.ipAddrLen;
    }
    private Boolean isIpv4Only;

    /**
     * VRRPv2 only supports IPv4
     */
    public Boolean isIpv4Only() {
        if (this.isIpv4Only != null)
            return this.isIpv4Only;
        this.isIpv4Only = version() == 2;
        return this.isIpv4Only;
    }
    private Boolean isIpv6Capable;

    /**
     * VRRPv3 supports both IPv4 and IPv6
     */
    public Boolean isIpv6Capable() {
        if (this.isIpv6Capable != null)
            return this.isIpv6Capable;
        this.isIpv6Capable = version() == 3;
        return this.isIpv6Capable;
    }
    private Boolean isValidType;

    /**
     * Validation that type is Advertisement (1)
     */
    public Boolean isValidType() {
        if (this.isValidType != null)
            return this.isValidType;
        this.isValidType = type() == 1;
        return this.isValidType;
    }
    private Boolean isValidVersion;

    /**
     * Validation that version is either 2 or 3
     */
    public Boolean isValidVersion() {
        if (this.isValidVersion != null)
            return this.isValidVersion;
        this.isValidVersion =  ((version() == 2) || (version() == 3)) ;
        return this.isValidVersion;
    }
    private Integer maxAdvertIntCentiseconds;

    /**
     * Maximum advertisement interval in centiseconds (VRRPv3)
     */
    public Integer maxAdvertIntCentiseconds() {
        if (this.maxAdvertIntCentiseconds != null)
            return this.maxAdvertIntCentiseconds;
        if (version() == 3) {
            this.maxAdvertIntCentiseconds = ((Number) (authTypeOrMaxAdvertIntHigh() << 8 | advertIntOrMaxAdvertIntLow())).intValue();
        }
        return this.maxAdvertIntCentiseconds;
    }
    private Double maxAdvertIntSeconds;

    /**
     * Maximum advertisement interval converted to seconds (VRRPv3)
     */
    public Double maxAdvertIntSeconds() {
        if (this.maxAdvertIntSeconds != null)
            return this.maxAdvertIntSeconds;
        if (version() == 3) {
            this.maxAdvertIntSeconds = ((Number) (maxAdvertIntCentiseconds() / 100.0)).doubleValue();
        }
        return this.maxAdvertIntSeconds;
    }
    private Integer type;

    /**
     * Packet type (always 1 for Advertisement)
     */
    public Integer type() {
        if (this.type != null)
            return this.type;
        this.type = ((Number) (versionType() & 15)).intValue();
        return this.type;
    }
    private Integer version;

    /**
     * VRRP protocol version (2 or 3)
     */
    public Integer version() {
        if (this.version != null)
            return this.version;
        this.version = ((Number) (versionType() >> 4 & 15)).intValue();
        return this.version;
    }
    private int versionType;
    private int vrid;
    private int priority;
    private int numIpAddresses;
    private int authTypeOrMaxAdvertIntHigh;
    private int advertIntOrMaxAdvertIntLow;
    private int checksum;
    private List<IpAddress> ipAddresses;
    private byte[] authenticationData;
    private int ipVersion;
    private Vrrp _root;
    private KaitaiStruct _parent;

    /**
     * Bits 7..4 : version (2 or 3)
     * Bits 3..0 : type (1 = Advertisement)
     */
    public int versionType() { return versionType; }

    /**
     * Virtual Router Identifier (1-255)
     */
    public int vrid() { return vrid; }

    /**
     * Priority (0–255, 255 = router owns IP addresses)
     */
    public int priority() { return priority; }

    /**
     * Number of IP addresses (count IPvX addr in VRRPv2, 0 in VRRPv3 for IPv6)
     */
    public int numIpAddresses() { return numIpAddresses; }

    /**
     * VRRPv2: Authentication Type (0=No Auth, 1=Simple, 2=IP Auth Header)
     * VRRPv3: High byte of Max Advertisement Interval
     */
    public int authTypeOrMaxAdvertIntHigh() { return authTypeOrMaxAdvertIntHigh; }

    /**
     * VRRPv2: Advertisement Interval in seconds
     * VRRPv3: Low byte of Max Advertisement Interval (in centiseconds)
     */
    public int advertIntOrMaxAdvertIntLow() { return advertIntOrMaxAdvertIntLow; }

    /**
     * VRRPv2: Checksum of VRRP message only
     * VRRPv3: Pseudo-header checksum (includes source IP)
     */
    public int checksum() { return checksum; }

    /**
     * VRRPv2: Always present (1+ IPv4 addresses)
     * VRRPv3 IPv4: Present (0+ IPv4 addresses)
     * VRRPv3 IPv6: Not present (addresses in IPv6 header)
     */
    public List<IpAddress> ipAddresses() { return ipAddresses; }

    /**
     * Authentication data (only in VRRPv2 with auth)
     */
    public byte[] authenticationData() { return authenticationData; }

    /**
     * IP version from parent packet (4 for IPv4, 6 for IPv6)
     */
    public int ipVersion() { return ipVersion; }
    public Vrrp _root() { return _root; }
    public KaitaiStruct _parent() { return _parent; }
}
