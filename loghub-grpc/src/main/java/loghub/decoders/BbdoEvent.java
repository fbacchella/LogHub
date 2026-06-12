package loghub.decoders;

import java.util.Map;

public enum BbdoEvent {
    // NEB category (value=1), pb_ messages only (de_pb_* from events.hh)
    NEB_PB_SERVICE(BbdoCategory.NEB, 27, "com.centreon.broker.Service"),
    NEB_PB_ADAPTIVE_SERVICE(BbdoCategory.NEB, 28, "com.centreon.broker.AdaptiveService"),
    NEB_PB_SERVICE_STATUS(BbdoCategory.NEB, 29, "com.centreon.broker.ServiceStatus"),
    NEB_PB_HOST(BbdoCategory.NEB, 30, "com.centreon.broker.Host"),
    NEB_PB_ADAPTIVE_HOST(BbdoCategory.NEB, 31, "com.centreon.broker.AdaptiveHost"),
    NEB_PB_HOST_STATUS(BbdoCategory.NEB, 32, "com.centreon.broker.HostStatus"),
    NEB_PB_SEVERITY(BbdoCategory.NEB, 33, "com.centreon.broker.Severity"),
    NEB_PB_TAG(BbdoCategory.NEB, 34, "com.centreon.broker.Tag"),
    NEB_PB_COMMENT(BbdoCategory.NEB, 35, "com.centreon.broker.Comment"),
    NEB_PB_DOWNTIME(BbdoCategory.NEB, 36, "com.centreon.broker.Downtime"),
    NEB_PB_CUSTOM_VARIABLE(BbdoCategory.NEB, 37, "com.centreon.broker.CustomVariable"),
    NEB_PB_CUSTOM_VARIABLE_STATUS(BbdoCategory.NEB, 38, "com.centreon.broker.CustomVariableStatus"),
    NEB_PB_HOST_CHECK(BbdoCategory.NEB, 39, "com.centreon.broker.HostCheck"),
    NEB_PB_SERVICE_CHECK(BbdoCategory.NEB, 40, "com.centreon.broker.ServiceCheck"),
    NEB_PB_LOG_ENTRY(BbdoCategory.NEB, 41, "com.centreon.broker.LogEntry"),
    NEB_PB_INSTANCE_STATUS(BbdoCategory.NEB, 42, "com.centreon.broker.InstanceStatus"),
    NEB_PB_INSTANCE(BbdoCategory.NEB, 44, "com.centreon.broker.Instance"),
    NEB_PB_ACKNOWLEDGEMENT(BbdoCategory.NEB, 45, "com.centreon.broker.Acknowledgement"),
    NEB_PB_RESPONSIVE_INSTANCE(BbdoCategory.NEB, 46, "com.centreon.broker.ResponsiveInstance"),
    NEB_PB_HOST_GROUP(BbdoCategory.NEB, 49, "com.centreon.broker.HostGroup"),
    NEB_PB_HOST_GROUP_MEMBER(BbdoCategory.NEB, 50, "com.centreon.broker.HostGroupMember"),
    NEB_PB_SERVICE_GROUP(BbdoCategory.NEB, 51, "com.centreon.broker.ServiceGroup"),
    NEB_PB_SERVICE_GROUP_MEMBER(BbdoCategory.NEB, 52, "com.centreon.broker.ServiceGroupMember"),
    NEB_PB_HOST_PARENT(BbdoCategory.NEB, 53, "com.centreon.broker.HostParent"),
    NEB_PB_INSTANCE_CONFIGURATION(BbdoCategory.NEB, 54, "com.centreon.broker.InstanceConfiguration"),
    NEB_PB_ADAPTIVE_SERVICE_STATUS(BbdoCategory.NEB, 55, "com.centreon.broker.AdaptiveServiceStatus"),
    NEB_PB_ADAPTIVE_HOST_STATUS(BbdoCategory.NEB, 56, "com.centreon.broker.AdaptiveHostStatus"),
    NEB_PB_AGENT_STATS(BbdoCategory.NEB, 57, "com.centreon.broker.AgentStats"),
    NEB_PB_UNKNOWN_HOST(BbdoCategory.NEB, 58, "com.centreon.broker.UnknownHost"),
    // BBDO category (value=2)
    BBDO_PB_ACK(BbdoCategory.BBDO, 8, "com.centreon.broker.Ack"),
    BBDO_PB_STOP(BbdoCategory.BBDO, 9, "com.centreon.broker.Stop"),
    BBDO_WELCOME(BbdoCategory.BBDO, 7, "com.centreon.broker.Welcome"),
    // Storage category (value=3), pb_ messages only (de_pb_* from events.hh)
    STORAGE_PB_METRIC(BbdoCategory.STORAGE, 9, "com.centreon.broker.Metric"),
    STORAGE_PB_STATUS(BbdoCategory.STORAGE, 10, "com.centreon.broker.Status"),
    STORAGE_PB_INDEX_MAPPING(BbdoCategory.STORAGE, 11, "com.centreon.broker.IndexMapping"),
    STORAGE_PB_METRIC_MAPPING(BbdoCategory.STORAGE, 12, "com.centreon.broker.MetricMapping"),
    // BAM category (value=6), pb_ messages only (de_pb_* from events.hh)
    BAM_PB_INHERITED_DOWNTIME(BbdoCategory.BAM, 18, "com.centreon.broker.InheritedDowntime"),
    BAM_PB_BA_STATUS(BbdoCategory.BAM, 19, "com.centreon.broker.BaStatus"),
    BAM_PB_BA_EVENT(BbdoCategory.BAM, 20, "com.centreon.broker.BaEvent"),
    BAM_PB_KPI_EVENT(BbdoCategory.BAM, 21, "com.centreon.broker.KpiEvent"),
    BAM_PB_DIMENSION_BV_EVENT(BbdoCategory.BAM, 22, "com.centreon.broker.DimensionBvEvent"),
    BAM_PB_DIMENSION_BA_BV_RELATION_EVENT(BbdoCategory.BAM, 23, "com.centreon.broker.DimensionBaBvRelationEvent"),
    BAM_PB_DIMENSION_TIMEPERIOD(BbdoCategory.BAM, 24, "com.centreon.broker.DimensionTimeperiod"),
    BAM_PB_DIMENSION_BA_EVENT(BbdoCategory.BAM, 25, "com.centreon.broker.DimensionBaEvent"),
    BAM_PB_DIMENSION_KPI_EVENT(BbdoCategory.BAM, 26, "com.centreon.broker.DimensionKpiEvent"),
    BAM_PB_KPI_STATUS(BbdoCategory.BAM, 27, "com.centreon.broker.KpiStatus"),
    BAM_PB_BA_DURATION_EVENT(BbdoCategory.BAM, 28, "com.centreon.broker.BaDurationEvent"),
    BAM_PB_DIMENSION_BA_TIMEPERIOD_RELATION(BbdoCategory.BAM, 29, "com.centreon.broker.DimensionBaTimeperiodRelation"),
    BAM_PB_DIMENSION_TRUNCATE_TABLE_SIGNAL(BbdoCategory.BAM, 30, "com.centreon.broker.DimensionTruncateTableSignal");

    public final BbdoCategory category;
    public final int message;
    public final String messageName;

    BbdoEvent(BbdoCategory category, int message, String messageName) {
        this.category = category;
        this.message = message;
        this.messageName = messageName;
    }

    private static final Map<Integer, BbdoEvent> BY_ID;
    static {
        Map<Integer, BbdoEvent> map = new java.util.HashMap<>();
        for (BbdoEvent e : values()) {
            map.put((e.category.value << 16) | e.message, e);
        }
        BY_ID = Map.copyOf(map);
    }

    public static BbdoEvent fromId(int id) {
        return BY_ID.get(id);
    }

    @Override
    public String toString() {
        return name() + "(" + category.name() + "/" + message + ")";
    }
}
