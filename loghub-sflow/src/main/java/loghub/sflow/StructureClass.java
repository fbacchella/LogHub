package loghub.sflow;

public enum StructureClass {
    SAMPLE_DATA,
    FLOW_DATA,
    COUNTER_DATA;

    public static StructureClass resolve(String name) {
        switch (name) {
        case "sample_data":
            return SAMPLE_DATA;
        case "flow_data":
            return FLOW_DATA;
        case "counter_data":
            return COUNTER_DATA;
        default:
            throw new IllegalArgumentException("Invalid struct name " + name);
        }
    }
}
