package loghub.sflow;

import lombok.Getter;

@Getter
public class DataSource {
    public enum SourceType {
        UNKOWN("Unknown"),
        IF_INDEX("ifIndex"),
        SMON_VLAN_DATA_SOURCE("smonVlanDataSource"),
        ENT_PHYSICAL_ENTRY("entPhysicalEntry");
        private final String display;

        SourceType(String display) {
            this.display = display;
        }

        @Override
        public String toString() {
            return display;
        }
    }
    private final SourceType sourceType;
    private final int index;
    public DataSource(int value) {
        switch (value >> 24) {
        case 0:
            sourceType = SourceType.IF_INDEX;
            break;
        case 1:
            sourceType = SourceType.SMON_VLAN_DATA_SOURCE;
            break;
        case 2:
            sourceType = SourceType.ENT_PHYSICAL_ENTRY;
            break;
        default:
            sourceType = SourceType.UNKOWN;
        }
        index = value & ((2 << 23) - 1);
    }
    @Override
    public String toString() {
        return sourceType + "/" + index;
    }
}
