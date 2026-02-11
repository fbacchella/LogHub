meta:
  id: igmp
  title: Internet Group Management Protocol (IGMP)
  license: CC0-1.0
  endian: be

doc: |
    Internet Group Management Protocol (IGMP) is used by IPv4 hosts
    and adjacent routers to establish multicast group memberships.

    Supports IGMPv1 (RFC 1112), IGMPv2 (RFC 2236), and IGMPv3 (RFC 3376).

    IGMP is encapsulated directly in IP datagrams with protocol number 2.

doc-ref:
  - https://www.rfc-editor.org/rfc/rfc1112 (IGMPv1)
  - https://www.rfc-editor.org/rfc/rfc2236 (IGMPv2)
  - https://www.rfc-editor.org/rfc/rfc3376 (IGMPv3)
  - https://www.rfc-editor.org/rfc/rfc3488 (RGMP)
  - https://www.rfc-editor.org/rfc/rfc4286 (Multicast Router Discovery)

seq:
  - id: type
    type: u1
    enum: igmp_type
    doc: IGMP message type

  - id: body
    type:
      switch-on: type
      cases:
        'igmp_type::membership_query': igmp_query
        'igmp_type::membership_report_v1': igmp_report_v1_v2
        'igmp_type::membership_report_v2': igmp_report_v1_v2
        'igmp_type::leave_group': igmp_leave
        'igmp_type::membership_report_v3': igmp_report_v3
        'igmp_type::rgmp_leave': rgmp
        'igmp_type::rgmp_join': rgmp
        'igmp_type::rgmp_bye': rgmp
        'igmp_type::rgmp_hello': rgmp

enums:
  igmp_type:
    0x11: membership_query         # Query (IGMPv1, v2, v3)
    0x12: membership_report_v1     # IGMPv1 Report
    0x13: dvmrp                    # DVMRP (RFC 1075)
    0x14: pim_v1                   # PIMv1
    0x15: cisco_trace              # Cisco Trace
    0x16: membership_report_v2     # IGMPv2 Report
    0x17: leave_group              # IGMPv2 Leave Group
    0x1e: mtrace_response          # Multicast Traceroute Response
    0x1f: mtrace_query             # Multicast Traceroute Query
    0x22: membership_report_v3     # IGMPv3 Report
    0x30: mcast_router_adv         # Multicast Router Advertisement (RFC 4286)
    0x31: mcast_router_sol         # Multicast Router Solicitation (RFC 4286)
    0x32: mcast_router_term        # Multicast Router Termination (RFC 4286)
    0xfc: rgmp_leave               # RGMP Leave Group
    0xfd: rgmp_join                # RGMP Join Group
    0xfe: rgmp_bye                 # RGMP Bye
    0xff: rgmp_hello               # RGMP Hello

  rgmp_code:
    0x01: leave
    0x02: join
    0x03: bye
    0x04: hello

  record_type:
    1: mode_is_include             # MODE_IS_INCLUDE
    2: mode_is_exclude             # MODE_IS_EXCLUDE
    3: change_to_include_mode      # CHANGE_TO_INCLUDE_MODE
    4: change_to_exclude_mode      # CHANGE_TO_EXCLUDE_MODE
    5: allow_new_sources           # ALLOW_NEW_SOURCES
    6: block_old_sources           # BLOCK_OLD_SOURCES

types:
  # ============================================================================
  # IGMPv1, IGMPv2, and IGMPv3 Query
  # ============================================================================
  igmp_query:
    doc: |
      Query message used by multicast routers to discover which multicast
      groups have members on attached networks.
    seq:
      - id: max_resp_time
        type: u1
        doc: |
          Maximum response time in tenths of a second (IGMPv2+).
          Must be 0 for IGMPv1.

      - id: checksum
        type: u2
        doc: Checksum of the entire IGMP message

      - id: group_address
        type: u4
        doc: |
          Multicast group address being queried.
          0.0.0.0 for a general query (all groups).
          Specific group address for group-specific query (IGMPv2+).

      - id: additional_data
        type: igmp_query_v3_additional
        if: _io.size > 8
        doc: Additional data for IGMPv3 queries

    instances:
      is_general_query:
        value: group_address == 0
        doc: True if this is a general query (all groups)

      max_resp_time_seconds:
        value: |
          max_resp_time < 128 ? max_resp_time / 10.0 :
          (((max_resp_time & 0x0f) | 0x10) << (((max_resp_time >> 4) & 0x07) + 3)) / 10.0
        doc: Maximum response time in seconds

      group_address_str:
        value: |
          ((group_address >> 24) & 0xff).to_s + "." +
          ((group_address >> 16) & 0xff).to_s + "." +
          ((group_address >> 8) & 0xff).to_s + "." +
          (group_address & 0xff).to_s
        doc: Group address as dotted decimal string

  igmp_query_v3_additional:
    doc: Additional fields for IGMPv3 queries
    seq:
      - id: resv_s_qrv
        type: u1
        doc: |
          Bits 7-4: Reserved (must be 0)
          Bit 3: S (Suppress Router-Side Processing)
          Bits 2-0: QRV (Querier's Robustness Variable)

      - id: qqic
        type: u1
        doc: Querier's Query Interval Code

      - id: number_of_sources
        type: u2
        doc: Number of source addresses in this query

      - id: source_addresses
        type: u4
        repeat: expr
        repeat-expr: number_of_sources
        doc: List of source addresses

    instances:
      suppress_router_side_processing:
        value: (resv_s_qrv & 0x08) != 0
        doc: S flag - suppress router-side processing

      querier_robustness_variable:
        value: resv_s_qrv & 0x07
        doc: QRV - Querier's Robustness Variable

      query_interval_seconds:
        value: |
          qqic < 128 ? qqic :
          (((qqic & 0x0f) | 0x10) << (((qqic >> 4) & 0x07) + 3))
        doc: Query interval in seconds

  # ============================================================================
  # IGMPv1 and IGMPv2 Membership Report
  # ============================================================================
  igmp_report_v1_v2:
    doc: |
      Membership Report message (IGMPv1 and IGMPv2).
      Sent by hosts to report multicast group membership.
    seq:
      - id: max_resp_time
        type: u1
        doc: Must be 0 for reports

      - id: checksum
        type: u2
        doc: Checksum of the entire IGMP message

      - id: group_address
        type: u4
        doc: Multicast group address being reported

    instances:
      group_address_str:
        value: |
          ((group_address >> 24) & 0xff).to_s + "." +
          ((group_address >> 16) & 0xff).to_s + "." +
          ((group_address >> 8) & 0xff).to_s + "." +
          (group_address & 0xff).to_s
        doc: Group address as dotted decimal string

  # ============================================================================
  # IGMPv2 Leave Group
  # ============================================================================
  igmp_leave:
    doc: |
      Leave Group message (IGMPv2).
      Sent by hosts when leaving a multicast group.
    seq:
      - id: max_resp_time
        type: u1
        doc: Must be 0 for leave messages

      - id: checksum
        type: u2
        doc: Checksum of the entire IGMP message

      - id: group_address
        type: u4
        doc: Multicast group address being left

    instances:
      group_address_str:
        value: |
          ((group_address >> 24) & 0xff).to_s + "." +
          ((group_address >> 16) & 0xff).to_s + "." +
          ((group_address >> 8) & 0xff).to_s + "." +
          (group_address & 0xff).to_s
        doc: Group address as dotted decimal string

  # ============================================================================
  # IGMPv3 Membership Report
  # ============================================================================
  igmp_report_v3:
    doc: |
      IGMPv3 Membership Report message.
      Allows hosts to specify source filtering for multicast groups.
    seq:
      - id: reserved
        type: u1
        doc: Reserved field, must be 0

      - id: checksum
        type: u2
        doc: Checksum of the entire IGMP message

      - id: reserved2
        type: u2
        doc: Reserved field, must be 0

      - id: number_of_group_records
        type: u2
        doc: Number of group records in this report

      - id: group_records
        type: group_record
        repeat: expr
        repeat-expr: number_of_group_records
        doc: List of group records

  group_record:
    doc: Single group record in an IGMPv3 report
    seq:
      - id: record_type
        type: u1
        enum: record_type
        doc: Type of group record

      - id: aux_data_len
        type: u1
        doc: Length of auxiliary data in 32-bit words

      - id: number_of_sources
        type: u2
        doc: Number of source addresses in this record

      - id: multicast_address
        type: u4
        doc: Multicast group address

      - id: source_addresses
        type: u4
        repeat: expr
        repeat-expr: number_of_sources
        doc: List of source addresses

      - id: auxiliary_data
        size: aux_data_len * 4
        if: aux_data_len > 0
        doc: Auxiliary data (optional)

  # ============================================================================
  # RGMP (RFC 3488)
  # ============================================================================
  rgmp:
    doc: |
      Router-port Group Management Protocol (RGMP).
      Used to constrain multicast traffic to only those ports that have
      routers that have expressed an interest in the traffic.
    seq:
      - id: code
        type: u1
        enum: rgmp_code
        doc: RGMP message type

      - id: checksum
        type: u2
        doc: Checksum of the entire RGMP message

      - id: group_address
        type: u4
        doc: Multicast group address

    instances:
      group_address_str:
        value: |
          ((group_address >> 24) & 0xff).to_s + "." +
          ((group_address >> 16) & 0xff).to_s + "." +
          ((group_address >> 8) & 0xff).to_s + "." +
          (group_address & 0xff).to_s
        doc: Group address as dotted decimal string
