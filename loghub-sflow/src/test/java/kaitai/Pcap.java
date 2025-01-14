package kaitai;// This is a generated file! Please edit source .ksy file and use kaitai-struct-compiler to rebuild

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import io.kaitai.struct.ByteBufferKaitaiStream;
import io.kaitai.struct.KaitaiStream;
import io.kaitai.struct.KaitaiStruct;


/**
 * PCAP (named after libpcap / winpcap) is a popular format for saving
 * network traffic grabbed by network sniffers. It is typically
 * produced by tools like [tcpdump](https://www.tcpdump.org/) or
 * [Wireshark](https://www.wireshark.org/).
 * @see <a href="https://wiki.wireshark.org/Development/LibpcapFileFormat">Source</a>
 */
public class Pcap extends KaitaiStruct {
    public static Pcap fromFile(String fileName) throws IOException {
        return new Pcap(new ByteBufferKaitaiStream(fileName));
    }

    public enum Linktype {
        NULL_LINKTYPE(0),
        ETHERNET(1),
        EXP_ETHERNET(2),
        AX25(3),
        PRONET(4),
        CHAOS(5),
        IEEE802_5(6),
        ARCNET_BSD(7),
        SLIP(8),
        PPP(9),
        FDDI(10),
        REDBACK_SMARTEDGE(32),
        PPP_HDLC(50),
        PPP_ETHER(51),
        SYMANTEC_FIREWALL(99),
        ATM_RFC1483(100),
        RAW(101),
        C_HDLC(104),
        IEEE802_11(105),
        ATM_CLIP(106),
        FRELAY(107),
        LOOP(108),
        ENC(109),
        NETBSD_HDLC(112),
        LINUX_SLL(113),
        LTALK(114),
        ECONET(115),
        IPFILTER(116),
        PFLOG(117),
        CISCO_IOS(118),
        IEEE802_11_PRISM(119),
        AIRONET_HEADER(120),
        IP_OVER_FC(122),
        SUNATM(123),
        RIO(124),
        PCI_EXP(125),
        AURORA(126),
        IEEE802_11_RADIOTAP(127),
        TZSP(128),
        ARCNET_LINUX(129),
        JUNIPER_MLPPP(130),
        JUNIPER_MLFR(131),
        JUNIPER_ES(132),
        JUNIPER_GGSN(133),
        JUNIPER_MFR(134),
        JUNIPER_ATM2(135),
        JUNIPER_SERVICES(136),
        JUNIPER_ATM1(137),
        APPLE_IP_OVER_IEEE1394(138),
        MTP2_WITH_PHDR(139),
        MTP2(140),
        MTP3(141),
        SCCP(142),
        DOCSIS(143),
        LINUX_IRDA(144),
        IBM_SP(145),
        IBM_SN(146),
        USER0(147),
        USER1(148),
        USER2(149),
        USER3(150),
        USER4(151),
        USER5(152),
        USER6(153),
        USER7(154),
        USER8(155),
        USER9(156),
        USER10(157),
        USER11(158),
        USER12(159),
        USER13(160),
        USER14(161),
        USER15(162),
        IEEE802_11_AVS(163),
        JUNIPER_MONITOR(164),
        BACNET_MS_TP(165),
        PPP_PPPD(166),
        JUNIPER_PPPOE(167),
        JUNIPER_PPPOE_ATM(168),
        GPRS_LLC(169),
        GPF_T(170),
        GPF_F(171),
        GCOM_T1E1(172),
        GCOM_SERIAL(173),
        JUNIPER_PIC_PEER(174),
        ERF_ETH(175),
        ERF_POS(176),
        LINUX_LAPD(177),
        JUNIPER_ETHER(178),
        JUNIPER_PPP(179),
        JUNIPER_FRELAY(180),
        JUNIPER_CHDLC(181),
        MFR(182),
        JUNIPER_VP(183),
        A429(184),
        A653_ICM(185),
        USB_FREEBSD(186),
        BLUETOOTH_HCI_H4(187),
        IEEE802_16_MAC_CPS(188),
        USB_LINUX(189),
        CAN20B(190),
        IEEE802_15_4_LINUX(191),
        PPI(192),
        IEEE802_16_MAC_CPS_RADIO(193),
        JUNIPER_ISM(194),
        IEEE802_15_4_WITHFCS(195),
        SITA(196),
        ERF(197),
        RAIF1(198),
        IPMB_KONTRON(199),
        JUNIPER_ST(200),
        BLUETOOTH_HCI_H4_WITH_PHDR(201),
        AX25_KISS(202),
        LAPD(203),
        PPP_WITH_DIR(204),
        C_HDLC_WITH_DIR(205),
        FRELAY_WITH_DIR(206),
        LAPB_WITH_DIR(207),
        IPMB_LINUX(209),
        FLEXRAY(210),
        MOST(211),
        LIN(212),
        X2E_SERIAL(213),
        X2E_XORAYA(214),
        IEEE802_15_4_NONASK_PHY(215),
        LINUX_EVDEV(216),
        GSMTAP_UM(217),
        GSMTAP_ABIS(218),
        MPLS(219),
        USB_LINUX_MMAPPED(220),
        DECT(221),
        AOS(222),
        WIHART(223),
        FC_2(224),
        FC_2_WITH_FRAME_DELIMS(225),
        IPNET(226),
        CAN_SOCKETCAN(227),
        IPV4(228),
        IPV6(229),
        IEEE802_15_4_NOFCS(230),
        DBUS(231),
        JUNIPER_VS(232),
        JUNIPER_SRX_E2E(233),
        JUNIPER_FIBRECHANNEL(234),
        DVB_CI(235),
        MUX27010(236),
        STANAG_5066_D_PDU(237),
        JUNIPER_ATM_CEMIC(238),
        NFLOG(239),
        NETANALYZER(240),
        NETANALYZER_TRANSPARENT(241),
        IPOIB(242),
        MPEG_2_TS(243),
        NG40(244),
        NFC_LLCP(245),
        PFSYNC(246),
        INFINIBAND(247),
        SCTP(248),
        USBPCAP(249),
        RTAC_SERIAL(250),
        BLUETOOTH_LE_LL(251),
        WIRESHARK_UPPER_PDU(252),
        NETLINK(253),
        BLUETOOTH_LINUX_MONITOR(254),
        BLUETOOTH_BREDR_BB(255),
        BLUETOOTH_LE_LL_WITH_PHDR(256),
        PROFIBUS_DL(257),
        PKTAP(258),
        EPON(259),
        IPMI_HPM_2(260),
        ZWAVE_R1_R2(261),
        ZWAVE_R3(262),
        WATTSTOPPER_DLM(263),
        ISO_14443(264),
        RDS(265),
        USB_DARWIN(266),
        OPENFLOW(267),
        SDLC(268),
        TI_LLN_SNIFFER(269),
        LORATAP(270),
        VSOCK(271),
        NORDIC_BLE(272),
        DOCSIS31_XRA31(273),
        ETHERNET_MPACKET(274),
        DISPLAYPORT_AUX(275),
        LINUX_SLL2(276),
        SERCOS_MONITOR(277),
        OPENVIZSLA(278),
        EBHSCR(279),
        VPP_DISPATCH(280),
        DSA_TAG_BRCM(281),
        DSA_TAG_BRCM_PREPEND(282),
        IEEE802_15_4_TAP(283),
        DSA_TAG_DSA(284),
        DSA_TAG_EDSA(285),
        ELEE(286),
        ZWAVE_SERIAL(287),
        USB_2_0(288),
        ATSC_ALP(289),
        ETW(290),
        NETANALYZER_NG(291),
        ZBOSS_NCP(292),
        USB_2_0_LOW_SPEED(293),
        USB_2_0_FULL_SPEED(294),
        USB_2_0_HIGH_SPEED(295),
        AUERSWALD_LOG(296),
        ZWAVE_TAP(297),
        SILABS_DEBUG_CHANNEL(298),
        FIRA_UCI(299);

        private final long id;
        Linktype(long id) { this.id = id; }
        public long id() { return id; }
        private static final Map<Long, Linktype> byId = new HashMap<Long, Linktype>(209);
        static {
            for (Linktype e : Linktype.values())
                byId.put(e.id(), e);
        }
        public static Linktype byId(long id) { return byId.get(id); }
    }

    public Pcap(KaitaiStream _io) {
        this(_io, null, null);
    }

    public Pcap(KaitaiStream _io, KaitaiStruct _parent) {
        this(_io, _parent, null);
    }

    public Pcap(KaitaiStream _io, KaitaiStruct _parent, Pcap _root) {
        super(_io);
        this._parent = _parent;
        this._root = _root == null ? this : _root;
        _read();
    }
    private void _read() {
        this.hdr = new Header(this._io, this, _root);
        this.packets = new ArrayList<Packet>();
        {
            int i = 0;
            while (!this._io.isEof()) {
                this.packets.add(new Packet(this._io, this, _root));
                i++;
            }
        }
    }

    /**
     * @see <a href="https://wiki.wireshark.org/Development/LibpcapFileFormat#Global_Header">Source</a>
     */
    public static class Header extends KaitaiStruct {
        public static Header fromFile(String fileName) throws IOException {
            return new Header(new ByteBufferKaitaiStream(fileName));
        }

        public Header(KaitaiStream _io) {
            this(_io, null, null);
        }

        public Header(KaitaiStream _io, Pcap _parent) {
            this(_io, _parent, null);
        }

        public Header(KaitaiStream _io, Pcap _parent, Pcap _root) {
            super(_io);
            this._parent = _parent;
            this._root = _root;
            _read();
        }
        private void _read() {
            this.magicNumber = this._io.readBytes(4);
            if (!(Arrays.equals(magicNumber(), new byte[] { -44, -61, -78, -95 }))) {
                throw new KaitaiStream.ValidationNotEqualError(new byte[] { -44, -61, -78, -95 }, magicNumber(), _io(), "/types/header/seq/0");
            }
            this.versionMajor = this._io.readU2le();
            if (!(versionMajor() == 2)) {
                throw new KaitaiStream.ValidationNotEqualError(2, versionMajor(), _io(), "/types/header/seq/1");
            }
            this.versionMinor = this._io.readU2le();
            this.thiszone = this._io.readS4le();
            this.sigfigs = this._io.readU4le();
            this.snaplen = this._io.readU4le();
            this.network = Linktype.byId(this._io.readU4le());
        }
        private byte[] magicNumber;
        private int versionMajor;
        private int versionMinor;
        private int thiszone;
        private long sigfigs;
        private long snaplen;
        private Linktype network;
        private Pcap _root;
        private Pcap _parent;
        public byte[] magicNumber() { return magicNumber; }
        public int versionMajor() { return versionMajor; }
        public int versionMinor() { return versionMinor; }

        /**
         * Correction time in seconds between UTC and the local
         * timezone of the following packet header timestamps.
         */
        public int thiszone() { return thiszone; }

        /**
         * In theory, the accuracy of time stamps in the capture; in
         * practice, all tools set it to 0.
         */
        public long sigfigs() { return sigfigs; }

        /**
         * The "snapshot length" for the capture (typically 65535 or
         * even more, but might be limited by the user), see: incl_len
         * vs. orig_len.
         */
        public long snaplen() { return snaplen; }

        /**
         * Link-layer header type, specifying the type of headers at
         * the beginning of the packet.
         */
        public Linktype network() { return network; }
        public Pcap _root() { return _root; }
        public Pcap _parent() { return _parent; }
    }

    /**
     * @see <a href="https://wiki.wireshark.org/Development/LibpcapFileFormat#Record_.28Packet.29_Header">Source</a>
     */
    public static class Packet extends KaitaiStruct {
        public static Packet fromFile(String fileName) throws IOException {
            return new Packet(new ByteBufferKaitaiStream(fileName));
        }

        public Packet(KaitaiStream _io) {
            this(_io, null, null);
        }

        public Packet(KaitaiStream _io, Pcap _parent) {
            this(_io, _parent, null);
        }

        public Packet(KaitaiStream _io, Pcap _parent, Pcap _root) {
            super(_io);
            this._parent = _parent;
            this._root = _root;
            _read();
        }
        private void _read() {
            this.tsSec = this._io.readU4le();
            this.tsUsec = this._io.readU4le();
            this.inclLen = this._io.readU4le();
            this.origLen = this._io.readU4le();
            {
                Linktype on = _root().hdr().network();
                if (on != null) {
                    switch (_root().hdr().network()) {
                    case PPI: {
                        this._raw_body = this._io.readBytes((inclLen() < _root().hdr().snaplen() ? inclLen() : _root().hdr().snaplen()));
                        KaitaiStream _io__raw_body = new ByteBufferKaitaiStream(_raw_body);
                        this.body = new PacketPpi(_io__raw_body);
                        break;
                    }
                    case ETHERNET: {
                        this._raw_body = this._io.readBytes((inclLen() < _root().hdr().snaplen() ? inclLen() : _root().hdr().snaplen()));
                        KaitaiStream _io__raw_body = new ByteBufferKaitaiStream(_raw_body);
                        this.body = new EthernetFrame(_io__raw_body);
                        break;
                    }
                    default: {
                        this.body = this._io.readBytes((inclLen() < _root().hdr().snaplen() ? inclLen() : _root().hdr().snaplen()));
                        break;
                    }
                    }
                } else {
                    this.body = this._io.readBytes((inclLen() < _root().hdr().snaplen() ? inclLen() : _root().hdr().snaplen()));
                }
            }
        }
        private long tsSec;
        private long tsUsec;
        private long inclLen;
        private long origLen;
        private Object body;
        private Pcap _root;
        private Pcap _parent;
        private byte[] _raw_body;
        public long tsSec() { return tsSec; }
        public long tsUsec() { return tsUsec; }

        /**
         * Number of bytes of packet data actually captured and saved in the file.
         */
        public long inclLen() { return inclLen; }

        /**
         * Length of the packet as it appeared on the network when it was captured.
         */
        public long origLen() { return origLen; }

        /**
         * @see <a href="https://wiki.wireshark.org/Development/LibpcapFileFormat#Packet_Data">Source</a>
         */
        public Object body() { return body; }
        public Pcap _root() { return _root; }
        public Pcap _parent() { return _parent; }
        public byte[] _raw_body() { return _raw_body; }
    }
    private Header hdr;
    private ArrayList<Packet> packets;
    private Pcap _root;
    private KaitaiStruct _parent;
    public Header hdr() { return hdr; }
    public ArrayList<Packet> packets() { return packets; }
    public Pcap _root() { return _root; }
    public KaitaiStruct _parent() { return _parent; }
}
