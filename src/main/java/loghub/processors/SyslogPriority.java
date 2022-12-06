package loghub.processors;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import loghub.ProcessorException;
import loghub.VariablePath;
import loghub.events.Event;
import lombok.Getter;
import lombok.Setter;

@FieldsProcessor.InPlace
public class SyslogPriority extends FieldsProcessor {
    
    private static final VariablePath ECSPATHFACILITY = VariablePath.of(new String[] {".", "log", "syslog", "facility"}) ;
    private static final VariablePath ECSPATHSEVERITY = VariablePath.of(new String[] {".", "log", "syslog", "severity"});
    private static final VariablePath ECSPATHPRIORITY = VariablePath.of(new String[] {".", "log", "syslog", "priority"});

    private String[] facilitiesNames = new String[]{"kernel",
                                                    "user-level",
                                                    "mail",
                                                    "daemon",
                                                    "security/authorization",
                                                    "syslogd",
                                                    "line printer",
                                                    "network news",
                                                    "uucp",
                                                    "clock",
                                                    "security/authorization",
                                                    "ftp",
                                                    "ntp",
                                                    "log audit",
                                                    "log alert",
                                                    "clock",
                                                    "local0",
                                                    "local1",
                                                    "local2",
                                                    "local3",
                                                    "local4",
                                                    "local5",
                                                    "local6",
                                                    "local7",
    };

    private String[] severitiesNames = new String[] {"emergency",
                                                     "alert",
                                                     "critical",
                                                     "error",
                                                     "warning",
                                                     "notice",
                                                     "informational",
                                                     "debug",
    };

    @Getter @Setter
    private boolean resolve = true;
    @Getter @Setter
    private boolean ecs = false;

    @Override
    public Object fieldFunction(Event event, Object priorityObject)
                    throws ProcessorException {
        int priority;
        if (priorityObject instanceof String) {
            try {
                priority = Integer.parseInt((String) priorityObject);
            } catch (NumberFormatException e) {
                throw event.buildException("Not a number: " + priorityObject.toString());
            }
        } else if (priorityObject instanceof Number) {
            priority = ((Number) priorityObject).intValue();
        } else {
            throw event.buildException("Not a priority: " + Optional.ofNullable(priorityObject).map(Object::toString).orElse(null));
        }
        int facility = (priority >> 3);
        int severity = priority & 7;
        Optional<String> facilityName = Optional.empty();
        String severityName = null;
        if (resolve || ecs) {
            facilityName = Optional.of(priorityObject)
                                   .filter(f -> facility < 24)
                                   .map(f -> facilitiesNames[facility]);
            severityName = severitiesNames[severity];
        }
        Map<String, Object> infos = new HashMap<>(2);
        if (ecs && ! isInPlace()) {
            Map<String, Object> facilityEntry = new HashMap<>(2);
            facilityEntry.put("code", facility);
            facilityName.ifPresent(s -> facilityEntry.put("name", s));
            Map<String, Object> severityEntry = new HashMap<>(2);
            severityEntry.put("code", severity);
            severityEntry.put("name", severityName);
            event.putAtPath(ECSPATHPRIORITY, priority);
            event.putAtPath(ECSPATHFACILITY, facilityEntry);
            event.putAtPath(ECSPATHSEVERITY, severityEntry);
            return RUNSTATUS.NOSTORE;
        } else if (ecs) {
            Map<String, Object> facilityEntry = new HashMap<>(2);
            facilityEntry.put("code", facility);
            facilityName.ifPresent(s -> facilityEntry.put("name", s));
            Map<String, Object> severityEntry = new HashMap<>(2);
            severityEntry.put("code", severity);
            severityEntry.put("name", severityName);
            Map<String, Map<String, Object>> logEntry = new HashMap<>(2);
            logEntry.put("severity", severityEntry);
            logEntry.put("facility", facilityEntry);
            return logEntry;
        } else if (resolve) {
            infos.put("facility", facilityName.orElse(Integer.toString(facility)));
            infos.put("severity", severityName);
        } else {
            infos.put("facility", facility);
            infos.put("severity", severity);
        }
        return infos;
    }

    @Override
    public String getName() {
        return null;
    }

    /**
     * @return the severity
     */
    public String[] getSeverities() {
        return Arrays.copyOf(severitiesNames, severitiesNames.length);
    }

    /**
     * @param severitiesNames the severity to set
     */
    public void setSeverities(String[] severitiesNames) {
        this.severitiesNames = Arrays.copyOf(severitiesNames, severitiesNames.length);
    }

    /**
     * @return the facility
     */
    public String[] getFacilities() {
        return Arrays.copyOf(facilitiesNames, facilitiesNames.length);
    }

    /**
     * @param facilitiesNames the facility to set
     */
    public void setFacilities(String[] facilitiesNames) {
        this.facilitiesNames = Arrays.copyOf(facilitiesNames, facilitiesNames.length);
    }

}
