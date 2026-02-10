// This is a generated file! Please edit source .ksy file and use kaitai-struct-compiler to rebuild

package loghub.kaitai.parsers;

import io.kaitai.struct.ByteBufferKaitaiStream;
import io.kaitai.struct.KaitaiStruct;
import io.kaitai.struct.KaitaiStream;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;


/**
 * Internet Group Management Protocol (IGMP) is used by IPv4 hosts
 * and adjacent routers to establish multicast group memberships.
 * 
 * Supports IGMPv1 (RFC 1112), IGMPv2 (RFC 2236), and IGMPv3 (RFC 3376).
 * 
 * IGMP is encapsulated directly in IP datagrams with protocol number 2.
 * @see <a href="https://www.rfc-editor.org/rfc/rfc1112">(IGMPv1)</a>
 * @see <a href="https://www.rfc-editor.org/rfc/rfc2236">(IGMPv2)</a>
 * @see <a href="https://www.rfc-editor.org/rfc/rfc3376">(IGMPv3)</a>
 */
public class Igmp extends KaitaiStruct {
    public static Igmp fromFile(String fileName) throws IOException {
        return new Igmp(new ByteBufferKaitaiStream(fileName));
    }

    public enum IgmpType {
        MEMBERSHIP_QUERY(17),
        MEMBERSHIP_REPORT_V1(18),
        MEMBERSHIP_REPORT_V2(22),
        LEAVE_GROUP(23),
        MEMBERSHIP_REPORT_V3(34);

        private final long id;
        IgmpType(long id) { this.id = id; }
        public long id() { return id; }
        private static final Map<Long, IgmpType> byId = new HashMap<Long, IgmpType>(5);
        static {
            for (IgmpType e : IgmpType.values())
                byId.put(e.id(), e);
        }
        public static IgmpType byId(long id) { return byId.get(id); }
    }

    public enum RecordType {
        MODE_IS_INCLUDE(1),
        MODE_IS_EXCLUDE(2),
        CHANGE_TO_INCLUDE_MODE(3),
        CHANGE_TO_EXCLUDE_MODE(4),
        ALLOW_NEW_SOURCES(5),
        BLOCK_OLD_SOURCES(6);

        private final long id;
        RecordType(long id) { this.id = id; }
        public long id() { return id; }
        private static final Map<Long, RecordType> byId = new HashMap<Long, RecordType>(6);
        static {
            for (RecordType e : RecordType.values())
                byId.put(e.id(), e);
        }
        public static RecordType byId(long id) { return byId.get(id); }
    }

    public Igmp(KaitaiStream _io) {
        this(_io, null, null);
    }

    public Igmp(KaitaiStream _io, KaitaiStruct _parent) {
        this(_io, _parent, null);
    }

    public Igmp(KaitaiStream _io, KaitaiStruct _parent, Igmp _root) {
        super(_io);
        this._parent = _parent;
        this._root = _root == null ? this : _root;
        _read();
    }
    private void _read() {
        this.type = IgmpType.byId(this._io.readU1());
        {
            IgmpType on = type();
            if (on != null) {
                switch (type()) {
                case LEAVE_GROUP: {
                    this.body = new IgmpLeave(this._io, this, _root);
                    break;
                }
                case MEMBERSHIP_QUERY: {
                    this.body = new IgmpQuery(this._io, this, _root);
                    break;
                }
                case MEMBERSHIP_REPORT_V1: {
                    this.body = new IgmpReportV1V2(this._io, this, _root);
                    break;
                }
                case MEMBERSHIP_REPORT_V2: {
                    this.body = new IgmpReportV1V2(this._io, this, _root);
                    break;
                }
                case MEMBERSHIP_REPORT_V3: {
                    this.body = new IgmpReportV3(this._io, this, _root);
                    break;
                }
                }
            }
        }
    }

    public void _fetchInstances() {
        {
            IgmpType on = type();
            if (on != null) {
                switch (type()) {
                case LEAVE_GROUP: {
                    ((IgmpLeave) (this.body))._fetchInstances();
                    break;
                }
                case MEMBERSHIP_QUERY: {
                    ((IgmpQuery) (this.body))._fetchInstances();
                    break;
                }
                case MEMBERSHIP_REPORT_V1: {
                    ((IgmpReportV1V2) (this.body))._fetchInstances();
                    break;
                }
                case MEMBERSHIP_REPORT_V2: {
                    ((IgmpReportV1V2) (this.body))._fetchInstances();
                    break;
                }
                case MEMBERSHIP_REPORT_V3: {
                    ((IgmpReportV3) (this.body))._fetchInstances();
                    break;
                }
                }
            }
        }
    }

    /**
     * Single group record in an IGMPv3 report
     */
    public static class GroupRecord extends KaitaiStruct {
        public static GroupRecord fromFile(String fileName) throws IOException {
            return new GroupRecord(new ByteBufferKaitaiStream(fileName));
        }

        public GroupRecord(KaitaiStream _io) {
            this(_io, null, null);
        }

        public GroupRecord(KaitaiStream _io, Igmp.IgmpReportV3 _parent) {
            this(_io, _parent, null);
        }

        public GroupRecord(KaitaiStream _io, Igmp.IgmpReportV3 _parent, Igmp _root) {
            super(_io);
            this._parent = _parent;
            this._root = _root;
            _read();
        }
        private void _read() {
            this.recordType = Igmp.RecordType.byId(this._io.readU1());
            this.auxDataLen = this._io.readU1();
            this.numberOfSources = this._io.readU2be();
            this.multicastAddress = this._io.readU4be();
            this.sourceAddresses = new ArrayList<Long>();
            for (int i = 0; i < numberOfSources(); i++) {
                this.sourceAddresses.add(this._io.readU4be());
            }
            if (auxDataLen() > 0) {
                this.auxiliaryData = this._io.readBytes(auxDataLen() * 4);
            }
        }

        public void _fetchInstances() {
            for (int i = 0; i < this.sourceAddresses.size(); i++) {
            }
            if (auxDataLen() > 0) {
            }
        }
        private String multicastAddressStr;

        /**
         * Multicast address as dotted decimal string
         */
        public String multicastAddressStr() {
            if (this.multicastAddressStr != null)
                return this.multicastAddressStr;
            this.multicastAddressStr = (((((Long.toString(multicastAddress() >> 24 & 255) + ".") + Long.toString(multicastAddress() >> 16 & 255)) + ".") + Long.toString(multicastAddress() >> 8 & 255)) + ".") + Long.toString(multicastAddress() & 255);
            return this.multicastAddressStr;
        }
        private RecordType recordType;
        private int auxDataLen;
        private int numberOfSources;
        private long multicastAddress;
        private List<Long> sourceAddresses;
        private byte[] auxiliaryData;
        private Igmp _root;
        private Igmp.IgmpReportV3 _parent;

        /**
         * Type of group record
         */
        public RecordType recordType() { return recordType; }

        /**
         * Length of auxiliary data in 32-bit words
         */
        public int auxDataLen() { return auxDataLen; }

        /**
         * Number of source addresses in this record
         */
        public int numberOfSources() { return numberOfSources; }

        /**
         * Multicast group address
         */
        public long multicastAddress() { return multicastAddress; }

        /**
         * List of source addresses
         */
        public List<Long> sourceAddresses() { return sourceAddresses; }

        /**
         * Auxiliary data (optional)
         */
        public byte[] auxiliaryData() { return auxiliaryData; }
        public Igmp _root() { return _root; }
        public Igmp.IgmpReportV3 _parent() { return _parent; }
    }

    /**
     * Leave Group message (IGMPv2).
     * Sent by hosts when leaving a multicast group.
     */
    public static class IgmpLeave extends KaitaiStruct {
        public static IgmpLeave fromFile(String fileName) throws IOException {
            return new IgmpLeave(new ByteBufferKaitaiStream(fileName));
        }

        public IgmpLeave(KaitaiStream _io) {
            this(_io, null, null);
        }

        public IgmpLeave(KaitaiStream _io, Igmp _parent) {
            this(_io, _parent, null);
        }

        public IgmpLeave(KaitaiStream _io, Igmp _parent, Igmp _root) {
            super(_io);
            this._parent = _parent;
            this._root = _root;
            _read();
        }
        private void _read() {
            this.maxRespTime = this._io.readU1();
            this.checksum = this._io.readU2be();
            this.groupAddress = this._io.readU4be();
        }

        public void _fetchInstances() {
        }
        private String groupAddressStr;

        /**
         * Group address as dotted decimal string
         */
        public String groupAddressStr() {
            if (this.groupAddressStr != null)
                return this.groupAddressStr;
            this.groupAddressStr = (((((Long.toString(groupAddress() >> 24 & 255) + ".") + Long.toString(groupAddress() >> 16 & 255)) + ".") + Long.toString(groupAddress() >> 8 & 255)) + ".") + Long.toString(groupAddress() & 255);
            return this.groupAddressStr;
        }
        private int maxRespTime;
        private int checksum;
        private long groupAddress;
        private Igmp _root;
        private Igmp _parent;

        /**
         * Must be 0 for leave messages
         */
        public int maxRespTime() { return maxRespTime; }

        /**
         * Checksum of the entire IGMP message
         */
        public int checksum() { return checksum; }

        /**
         * Multicast group address being left
         */
        public long groupAddress() { return groupAddress; }
        public Igmp _root() { return _root; }
        public Igmp _parent() { return _parent; }
    }

    /**
     * Query message used by multicast routers to discover which multicast
     * groups have members on attached networks.
     */
    public static class IgmpQuery extends KaitaiStruct {
        public static IgmpQuery fromFile(String fileName) throws IOException {
            return new IgmpQuery(new ByteBufferKaitaiStream(fileName));
        }

        public IgmpQuery(KaitaiStream _io) {
            this(_io, null, null);
        }

        public IgmpQuery(KaitaiStream _io, Igmp _parent) {
            this(_io, _parent, null);
        }

        public IgmpQuery(KaitaiStream _io, Igmp _parent, Igmp _root) {
            super(_io);
            this._parent = _parent;
            this._root = _root;
            _read();
        }
        private void _read() {
            this.maxRespTime = this._io.readU1();
            this.checksum = this._io.readU2be();
            this.groupAddress = this._io.readU4be();
            if (_io().size() > 8) {
                this.additionalData = new IgmpQueryV3Additional(this._io, this, _root);
            }
        }

        public void _fetchInstances() {
            if (_io().size() > 8) {
                this.additionalData._fetchInstances();
            }
        }
        private String groupAddressStr;

        /**
         * Group address as dotted decimal string
         */
        public String groupAddressStr() {
            if (this.groupAddressStr != null)
                return this.groupAddressStr;
            this.groupAddressStr = (((((Long.toString(groupAddress() >> 24 & 255) + ".") + Long.toString(groupAddress() >> 16 & 255)) + ".") + Long.toString(groupAddress() >> 8 & 255)) + ".") + Long.toString(groupAddress() & 255);
            return this.groupAddressStr;
        }
        private Boolean isGeneralQuery;

        /**
         * True if this is a general query (all groups)
         */
        public Boolean isGeneralQuery() {
            if (this.isGeneralQuery != null)
                return this.isGeneralQuery;
            this.isGeneralQuery = groupAddress() == 0;
            return this.isGeneralQuery;
        }
        private Double maxRespTimeSeconds;

        /**
         * Maximum response time in seconds
         */
        public Double maxRespTimeSeconds() {
            if (this.maxRespTimeSeconds != null)
                return this.maxRespTimeSeconds;
            this.maxRespTimeSeconds = ((Number) ((maxRespTime() < 128 ? maxRespTime() / 10.0 : ((maxRespTime() & 15 | 16) << (maxRespTime() >> 4 & 7) + 3) / 10.0))).doubleValue();
            return this.maxRespTimeSeconds;
        }
        private int maxRespTime;
        private int checksum;
        private long groupAddress;
        private IgmpQueryV3Additional additionalData;
        private Igmp _root;
        private Igmp _parent;

        /**
         * Maximum response time in tenths of a second (IGMPv2+).
         * Must be 0 for IGMPv1.
         */
        public int maxRespTime() { return maxRespTime; }

        /**
         * Checksum of the entire IGMP message
         */
        public int checksum() { return checksum; }

        /**
         * Multicast group address being queried.
         * 0.0.0.0 for a general query (all groups).
         * Specific group address for group-specific query (IGMPv2+).
         */
        public long groupAddress() { return groupAddress; }

        /**
         * Additional data for IGMPv3 queries
         */
        public IgmpQueryV3Additional additionalData() { return additionalData; }
        public Igmp _root() { return _root; }
        public Igmp _parent() { return _parent; }
    }

    /**
     * Additional fields for IGMPv3 queries
     */
    public static class IgmpQueryV3Additional extends KaitaiStruct {
        public static IgmpQueryV3Additional fromFile(String fileName) throws IOException {
            return new IgmpQueryV3Additional(new ByteBufferKaitaiStream(fileName));
        }

        public IgmpQueryV3Additional(KaitaiStream _io) {
            this(_io, null, null);
        }

        public IgmpQueryV3Additional(KaitaiStream _io, Igmp.IgmpQuery _parent) {
            this(_io, _parent, null);
        }

        public IgmpQueryV3Additional(KaitaiStream _io, Igmp.IgmpQuery _parent, Igmp _root) {
            super(_io);
            this._parent = _parent;
            this._root = _root;
            _read();
        }
        private void _read() {
            this.resvSQrv = this._io.readU1();
            this.qqic = this._io.readU1();
            this.numberOfSources = this._io.readU2be();
            this.sourceAddresses = new ArrayList<Long>();
            for (int i = 0; i < numberOfSources(); i++) {
                this.sourceAddresses.add(this._io.readU4be());
            }
        }

        public void _fetchInstances() {
            for (int i = 0; i < this.sourceAddresses.size(); i++) {
            }
        }
        private Integer querierRobustnessVariable;

        /**
         * QRV - Querier's Robustness Variable
         */
        public Integer querierRobustnessVariable() {
            if (this.querierRobustnessVariable != null)
                return this.querierRobustnessVariable;
            this.querierRobustnessVariable = ((Number) (resvSQrv() & 7)).intValue();
            return this.querierRobustnessVariable;
        }
        private Integer queryIntervalSeconds;

        /**
         * Query interval in seconds
         */
        public Integer queryIntervalSeconds() {
            if (this.queryIntervalSeconds != null)
                return this.queryIntervalSeconds;
            this.queryIntervalSeconds = ((Number) ((qqic() < 128 ? qqic() : (qqic() & 15 | 16) << (qqic() >> 4 & 7) + 3))).intValue();
            return this.queryIntervalSeconds;
        }
        private Boolean suppressRouterSideProcessing;

        /**
         * S flag - suppress router-side processing
         */
        public Boolean suppressRouterSideProcessing() {
            if (this.suppressRouterSideProcessing != null)
                return this.suppressRouterSideProcessing;
            this.suppressRouterSideProcessing = (resvSQrv() & 8) != 0;
            return this.suppressRouterSideProcessing;
        }
        private int resvSQrv;
        private int qqic;
        private int numberOfSources;
        private List<Long> sourceAddresses;
        private Igmp _root;
        private Igmp.IgmpQuery _parent;

        /**
         * Bits 7-4: Reserved (must be 0)
         * Bit 3: S (Suppress Router-Side Processing)
         * Bits 2-0: QRV (Querier's Robustness Variable)
         */
        public int resvSQrv() { return resvSQrv; }

        /**
         * Querier's Query Interval Code
         */
        public int qqic() { return qqic; }

        /**
         * Number of source addresses in this query
         */
        public int numberOfSources() { return numberOfSources; }

        /**
         * List of source addresses
         */
        public List<Long> sourceAddresses() { return sourceAddresses; }
        public Igmp _root() { return _root; }
        public Igmp.IgmpQuery _parent() { return _parent; }
    }

    /**
     * Membership Report message (IGMPv1 and IGMPv2).
     * Sent by hosts to report multicast group membership.
     */
    public static class IgmpReportV1V2 extends KaitaiStruct {
        public static IgmpReportV1V2 fromFile(String fileName) throws IOException {
            return new IgmpReportV1V2(new ByteBufferKaitaiStream(fileName));
        }

        public IgmpReportV1V2(KaitaiStream _io) {
            this(_io, null, null);
        }

        public IgmpReportV1V2(KaitaiStream _io, Igmp _parent) {
            this(_io, _parent, null);
        }

        public IgmpReportV1V2(KaitaiStream _io, Igmp _parent, Igmp _root) {
            super(_io);
            this._parent = _parent;
            this._root = _root;
            _read();
        }
        private void _read() {
            this.maxRespTime = this._io.readU1();
            this.checksum = this._io.readU2be();
            this.groupAddress = this._io.readU4be();
        }

        public void _fetchInstances() {
        }
        private String groupAddressStr;

        /**
         * Group address as dotted decimal string
         */
        public String groupAddressStr() {
            if (this.groupAddressStr != null)
                return this.groupAddressStr;
            this.groupAddressStr = (((((Long.toString(groupAddress() >> 24 & 255) + ".") + Long.toString(groupAddress() >> 16 & 255)) + ".") + Long.toString(groupAddress() >> 8 & 255)) + ".") + Long.toString(groupAddress() & 255);
            return this.groupAddressStr;
        }
        private int maxRespTime;
        private int checksum;
        private long groupAddress;
        private Igmp _root;
        private Igmp _parent;

        /**
         * Must be 0 for reports
         */
        public int maxRespTime() { return maxRespTime; }

        /**
         * Checksum of the entire IGMP message
         */
        public int checksum() { return checksum; }

        /**
         * Multicast group address being reported
         */
        public long groupAddress() { return groupAddress; }
        public Igmp _root() { return _root; }
        public Igmp _parent() { return _parent; }
    }

    /**
     * IGMPv3 Membership Report message.
     * Allows hosts to specify source filtering for multicast groups.
     */
    public static class IgmpReportV3 extends KaitaiStruct {
        public static IgmpReportV3 fromFile(String fileName) throws IOException {
            return new IgmpReportV3(new ByteBufferKaitaiStream(fileName));
        }

        public IgmpReportV3(KaitaiStream _io) {
            this(_io, null, null);
        }

        public IgmpReportV3(KaitaiStream _io, Igmp _parent) {
            this(_io, _parent, null);
        }

        public IgmpReportV3(KaitaiStream _io, Igmp _parent, Igmp _root) {
            super(_io);
            this._parent = _parent;
            this._root = _root;
            _read();
        }
        private void _read() {
            this.reserved = this._io.readU1();
            this.checksum = this._io.readU2be();
            this.reserved2 = this._io.readU2be();
            this.numberOfGroupRecords = this._io.readU2be();
            this.groupRecords = new ArrayList<GroupRecord>();
            for (int i = 0; i < numberOfGroupRecords(); i++) {
                this.groupRecords.add(new GroupRecord(this._io, this, _root));
            }
        }

        public void _fetchInstances() {
            for (int i = 0; i < this.groupRecords.size(); i++) {
                this.groupRecords.get(((Number) (i)).intValue())._fetchInstances();
            }
        }
        private int reserved;
        private int checksum;
        private int reserved2;
        private int numberOfGroupRecords;
        private List<GroupRecord> groupRecords;
        private Igmp _root;
        private Igmp _parent;

        /**
         * Reserved field, must be 0
         */
        public int reserved() { return reserved; }

        /**
         * Checksum of the entire IGMP message
         */
        public int checksum() { return checksum; }

        /**
         * Reserved field, must be 0
         */
        public int reserved2() { return reserved2; }

        /**
         * Number of group records in this report
         */
        public int numberOfGroupRecords() { return numberOfGroupRecords; }

        /**
         * List of group records
         */
        public List<GroupRecord> groupRecords() { return groupRecords; }
        public Igmp _root() { return _root; }
        public Igmp _parent() { return _parent; }
    }
    private IgmpType type;
    private KaitaiStruct body;
    private Igmp _root;
    private KaitaiStruct _parent;

    /**
     * IGMP message type
     */
    public IgmpType type() { return type; }
    public KaitaiStruct body() { return body; }
    public Igmp _root() { return _root; }
    public KaitaiStruct _parent() { return _parent; }
}
