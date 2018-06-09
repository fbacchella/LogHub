package loghub.processors;

import java.util.HashMap;
import java.util.Map;

import loghub.Event;
import loghub.ProcessorException;

public class SyslogPriority extends FieldsProcessor {

    private  String[] facilitiesNames = new String[]{"kernel",
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
                                                     "invalid facility",
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

    private boolean resolve = true;

    @Override
    public Object fieldFunction(Event event, Object priorityObject)
                    throws ProcessorException {
        int priority;
        if(priorityObject instanceof String) {
            try {
                priority = Integer.parseInt((String) priorityObject);
            } catch (NumberFormatException e) {
                throw event.buildException("Not a number: " + priorityObject.toString());
            }
        } else if ( priorityObject instanceof Number) {
            priority = ((Number) priorityObject).intValue();
        } else {
            throw event.buildException("Not a priority: " + priorityObject.toString());
        }
        int facility = (priority >> 3);
        if (facility > 24) {
            facility = 24;
        }
        int severity = priority & 7;
        Map<String, Object> infos = new HashMap<>(2);
        if (resolve) {
            infos.put("facility", facilitiesNames[facility]);
            infos.put("severity", severitiesNames[severity]);
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
        return severitiesNames;
    }

    /**
     * @param severitiesNames the severity to set
     */
    public void setSeverities(String[] severitiesNames) {
        this.severitiesNames = severitiesNames;
    }

    /**
     * @return the facility
     */
    public String[] getFacilities() {
        return facilitiesNames;
    }

    /**
     * @param facilitiesNames the facility to set
     */
    public void setFacilities(String[] facilitiesNames) {
        this.facilitiesNames = facilitiesNames;
    }

    /**
     * @return the resolve
     */
    public boolean isResolve() {
        return resolve;
    }

    /**
     * @param resolve the resolve to set
     */
    public void setResolve(boolean resolve) {
        this.resolve = resolve;
    }

}
