meta:
  id: vrrp
  title: VRRP (Virtual Router Redundancy Protocol) Packet
  endian: be
  license: CC0-1.0
doc: |
  Supports both VRRP version 2 (RFC 3768) and version 3 (RFC 5798).
  Version 2 supports only IPv4, version 3 supports both IPv4 and IPv6.

params:
  - id: ip_version
    type: u1
    doc: IP version from parent packet (4 for IPv4, 6 for IPv6)

seq:
  - id: version_type
    type: u1
    doc: |
      Bits 7..4 : version (2 or 3)
      Bits 3..0 : type (1 = Advertisement)

  - id: vrid
    type: u1
    doc: Virtual Router Identifier (1-255)

  - id: priority
    type: u1
    doc: Priority (0–255, 255 = router owns IP addresses)

  - id: num_ip_addresses
    type: u1
    doc: Number of IP addresses (count IPvX addr in VRRPv2, 0 in VRRPv3 for IPv6)

  - id: auth_type_or_max_advert_int_high
    type: u1
    doc: |
      VRRPv2: Authentication Type (0=No Auth, 1=Simple, 2=IP Auth Header)
      VRRPv3: High byte of Max Advertisement Interval

  - id: advert_int_or_max_advert_int_low
    type: u1
    doc: |
      VRRPv2: Advertisement Interval in seconds
      VRRPv3: Low byte of Max Advertisement Interval (in centiseconds)

  - id: checksum
    type: u2
    doc: |
      VRRPv2: Checksum of VRRP message only
      VRRPv3: Pseudo-header checksum (includes source IP)

  - id: ip_addresses
    type: ip_address
    repeat: expr
    repeat-expr: num_ip_addresses
    if: version == 2 or (version == 3 and ip_version == 4)
    doc: |
      VRRPv2: Always present (1+ IPv4 addresses)
      VRRPv3 IPv4: Present (0+ IPv4 addresses)
      VRRPv3 IPv6: Not present (addresses in IPv6 header)

  - id: authentication_data
    size: 8
    if: version == 2 and auth_type != 0
    doc: Authentication data (only in VRRPv2 with auth)

types:
  ip_address:
    seq:
      - id: address
        size: _parent.ip_addr_len
        doc: IPv4 (4 bytes) or IPv6 (16 bytes) address

instances:
  version:
    value: (version_type >> 4) & 0x0f
    doc: VRRP protocol version (2 or 3)

  type:
    value: version_type & 0x0f
    doc: Packet type (always 1 for Advertisement)

  ip_addr_len:
    value: "version == 3 and ip_version == 6 ? 16 : 4"
    doc: |
      Length of each IP address in bytes
      VRRPv2: always 4 (IPv4 only)
      VRRPv3: 4 for IPv4, 16 for IPv6

  auth_type:
    value: auth_type_or_max_advert_int_high
    if: version == 2
    doc: Authentication type (VRRPv2 only)

  advert_int_seconds:
    value: advert_int_or_max_advert_int_low
    if: version == 2
    doc: Advertisement interval in seconds (VRRPv2)

  max_advert_int_centiseconds:
    value: (auth_type_or_max_advert_int_high << 8) | advert_int_or_max_advert_int_low
    if: version == 3
    doc: Maximum advertisement interval in centiseconds (VRRPv3)

  max_advert_int_seconds:
    value: max_advert_int_centiseconds / 100.0
    if: version == 3
    doc: Maximum advertisement interval converted to seconds (VRRPv3)

  is_valid_version:
    value: version == 2 or version == 3
    doc: Validation that version is either 2 or 3

  is_valid_type:
    value: type == 1
    doc: Validation that type is Advertisement (1)

  is_ipv4_only:
    value: version == 2
    doc: VRRPv2 only supports IPv4

  is_ipv6_capable:
    value: version == 3
    doc: VRRPv3 supports both IPv4 and IPv6
