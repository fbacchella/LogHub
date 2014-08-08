package loghub.configuration;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import loghub.Event;
import loghub.Receiver;
import loghub.Sender;
import loghub.Transformer;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.events.DocumentEndEvent;
import org.yaml.snakeyaml.events.DocumentStartEvent;
import org.yaml.snakeyaml.events.Event.ID;
import org.yaml.snakeyaml.events.MappingStartEvent;
import org.yaml.snakeyaml.events.ScalarEvent;
import org.yaml.snakeyaml.events.SequenceStartEvent;
import org.yaml.snakeyaml.events.StreamEndEvent;
import org.yaml.snakeyaml.events.StreamStartEvent;
import org.zeromq.ZMQ.Context;

public class Configuration {
    
    static private final String IN_SLOT = "in";
    static private final String OUT_SLOT = "out";
    static private final String PIPE_SLOT = "pipe";
    
    
    static private final class ObjectInfo {
        final String className;
        final Map<String, String> beans;
        ObjectInfo(String className, Map<String, String> beans) {
            this.className = className;
            this.beans = beans;
        }
    }

    private final Map<String, Event> eventQueue;
    private final Context context;
    private final Yaml yaml = new Yaml();
    //private Map<String, List<?>> content;
    private final Map<String, List<ObjectInfo>> slots = new HashMap<>();

    public Configuration(Map<String, Event> eventQueue, Context context) {
        this.eventQueue = eventQueue;
        this.context = context;

        slots.put(IN_SLOT, new ArrayList<ObjectInfo>());
        slots.put(OUT_SLOT, new ArrayList<ObjectInfo>());
        slots.put(PIPE_SLOT, new ArrayList<ObjectInfo>());
    }

    private <T extends org.yaml.snakeyaml.events.Event> T strictConvert(Class<T> clazz, org.yaml.snakeyaml.events.Event e) {
        if(! e.getClass().isAssignableFrom(clazz)) {
            throw new ClassCastException("Syntax error, excepted " +  clazz.getCanonicalName() + ", got " + e.getClass() + " at" +  e.getStartMark());
        }
        @SuppressWarnings("unchecked")
        T buffer = (T) e;
        return buffer;
    }

    public void parse(String fileName) {
        try {

            Reader r = new FileReader(fileName);
            org.yaml.snakeyaml.events.Event next;
            Iterator<org.yaml.snakeyaml.events.Event> i = yaml.parse(r).iterator();
            try {
                next = strictConvert(StreamStartEvent.class, i.next());
                next = strictConvert(DocumentStartEvent.class, i.next());
                next = strictConvert(MappingStartEvent.class, i.next());
                //Iterating in, out, pipe
                while(i.hasNext()) {
                    next = i.next();
                    // End of slots
                    if(next.is(ID.MappingEnd)) {
                        break;
                    } else if(next.is(ID.Scalar)) {
                        ScalarEvent e4 = strictConvert(ScalarEvent.class, next);
                        String slotName = e4.getValue();
                        if(! slots.containsKey(slotName)) {
                            throw new RuntimeException("Invalid slot name '" + slotName + "' at " + e4.getStartMark());
                        }
                        next = i.next();
                        // A single class for the slot
                        if(next.is(ID.Scalar)) {
                            ScalarEvent e5 = strictConvert(ScalarEvent.class, next);
                            Map<String, String> beans = Collections.emptyMap();
                            ObjectInfo oi = new ObjectInfo(e5.getValue(), beans);
                            slots.get(slotName).add(oi);
                        // The slot contains a sequence of classes
                        } else if(next.is(ID.SequenceStart)){
                            next = strictConvert(SequenceStartEvent.class, next);
                            while(i.hasNext()) {
                                next = i.next();
                                // The class contains no beans
                                if(next.is(ID.Scalar)) {
                                    ScalarEvent e6 = strictConvert(ScalarEvent.class, next);
                                    Map<String, String> beans = Collections.emptyMap();
                                    slots.get(slotName).add(new ObjectInfo(e6.getValue(), beans));
                                // Resolve the beans for the sequence
                                } else if(next.is(ID.MappingStart)) {
                                    next = strictConvert(MappingStartEvent.class, next);
                                    ScalarEvent e7 = strictConvert(ScalarEvent.class, i.next());
                                    String className = e7.getValue();
                                    Map<String, String> beans = new HashMap<>();
                                    next = strictConvert(MappingStartEvent.class, i.next());
                                    while(i.hasNext()) {
                                        next = i.next();
                                        if(next.is(ID.MappingEnd)) {
                                            break;
                                        } else {
                                            ScalarEvent e9 = strictConvert(ScalarEvent.class, next);
                                            ScalarEvent e10 = strictConvert(ScalarEvent.class, i.next());
                                            beans.put(e9.getValue(), e10.getValue());
                                        }
                                    }
                                    slots.get(slotName).add(new ObjectInfo(className, beans));
                                } else if(next.is(ID.SequenceEnd)) {
                                    break;
                                }
                            }
                        } else if ( next.is(ID.MappingEnd)){
                            break;
                        }                        
                    }
                }
                next = strictConvert(DocumentEndEvent.class, i.next());
                next = strictConvert(StreamEndEvent.class, i.next());
            } catch (Exception e1) {
                e1.printStackTrace();
            }
            System.out.println(slots);

            //Object parsed = yaml.load(new FileInputStream(fileName));
            //content = (Map<String, List<?>>) parsed;
            //            System.out.println(content);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    public Transformer[][] getTransformers() {
        List<ObjectInfo> descriptions = slots.get(PIPE_SLOT);
        Transformer[][] transformers = new Transformer[descriptions.size()][];
        int i = 0;
        for(ObjectInfo oi: descriptions) {
            transformers[i++] = TransformerBuilder.create(oi.className, eventQueue, oi.beans);
        }
        //        Transformer[] logger = TransformerBuilder.create(Log.class.getCanonicalName(), eventQueue, empty);
        //        Map<String, String> grooveBeans = new HashMap<>(1);
        //        grooveBeans.put("script", "println event");
        //        grooveBeans.put("threads", "2");
        //        Transformer[] groovies = TransformerBuilder.create(Groovy.class.getCanonicalName(), eventQueue, grooveBeans);
        //Transformer[][] transformers = new Transformer[][] {
                //                logger,
                //                groovies,
        //};
        //System.out.println(Arrays.toString(transformers));
        return transformers;

    }

    public Receiver[] getReceivers(String inEndpoint) {
        List<ObjectInfo> descriptions = slots.get(IN_SLOT);
        Receiver[] receivers = new Receiver[descriptions.size()];
        int i = 0;
        for(ObjectInfo oi: descriptions) {
            try {
                String className = oi.className;
                @SuppressWarnings("unchecked")
                Class<Receiver> cl = (Class<Receiver>) getClass().getClassLoader().loadClass(className);
                Constructor<Receiver> c = cl.getConstructor(Context.class, String.class, Map.class);
                Receiver r = c.newInstance(context, inEndpoint, eventQueue);
                receivers[i++] = r;
                for(Map.Entry<String, String> bean: oi.beans.entrySet()) {
                    BeansManager.beanSetter(r, bean.getKey(), bean.getValue());
                }
            } catch (ClassNotFoundException|NoSuchMethodException|SecurityException|InstantiationException|IllegalAccessException|IllegalArgumentException|InvocationTargetException  e) {
                e.printStackTrace();
            }
        }
        return receivers;

//        for(Object inelement: content.get("in")) {
//            try {
//                String className = null;
//                Map<String, String> beans = Collections.emptyMap();
//                if(inelement instanceof String) {
//                    className = (String) inelement;
//
//                } else if (inelement instanceof Map) {
//                    Map<String, Map<?, ?>> object =  (Map<String, Map<?, ?>>) inelement;
//                    className = (String) object.keySet().toArray()[0];
//                    beans = (Map<String, String>) object.get(className);
//                }
//                Class<Receiver> cl = (Class<Receiver>) getClass().getClassLoader().loadClass(className);
//                Constructor<Receiver> c = cl.getConstructor(Context.class, String.class, Map.class);
//                receivers.add(c.newInstance(context, inEndpoint, eventQueue));
//                System.out.println(inelement);
//                System.out.println(className);
//            } catch (ClassNotFoundException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            } catch (NoSuchMethodException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            } catch (SecurityException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            } catch (InstantiationException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            } catch (IllegalAccessException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            } catch (IllegalArgumentException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            } catch (InvocationTargetException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            }
//        }
//
//
//        String[] receiversNames = new String[] {
//                "loghub.receivers.Log4JZMQ",
//                "loghub.receivers.SnmpTrap"
//        };
//        //        Receiver[] receivers = new Receiver[receiversNames.length];
//        //        for(int i=0; i < receiversNames.length; i++) {
//        //            String receiverName = receiversNames[i];
//        //            try {
//        //                @SuppressWarnings("unchecked")
//        //                Class<Receiver> cl = (Class<Receiver>) getClass().getClassLoader().loadClass(receiverName);
//        //                Constructor<Receiver> c = cl.getConstructor(Context.class, String.class, Map.class);
//        //                receivers[i] = c.newInstance(context, inEndpoint, eventQueue);
//        //            } catch (ClassNotFoundException|NoSuchMethodException|SecurityException|InstantiationException|IllegalAccessException|IllegalArgumentException|InvocationTargetException e) {
//        //                e.printStackTrace();
//        //            }
//        //        }
//        Receiver[] buffer = new Receiver[receivers.size()];
//        receivers.toArray(buffer);
//        System.out.println(receivers);
    }

    public Sender[] getSenders(String outEndpoint) {
        List<ObjectInfo> descriptions = slots.get(OUT_SLOT);
        Sender[] senders = new Sender[descriptions.size()];
        int i = 0;
        for(ObjectInfo oi: descriptions) {
            try {
                String className = oi.className;
                @SuppressWarnings("unchecked")
                Class<Sender> cl = (Class<Sender>) getClass().getClassLoader().loadClass(className);
                Constructor<Sender> c = cl.getConstructor(Context.class, String.class, Map.class);
                Sender r = c.newInstance(context, outEndpoint, eventQueue);
                senders[i++] = r;
                for(Map.Entry<String, String> bean: oi.beans.entrySet()) {
                    BeansManager.beanSetter(r, bean.getKey(), bean.getValue());
                }
            } catch (ClassNotFoundException|NoSuchMethodException|SecurityException|InstantiationException|IllegalAccessException|IllegalArgumentException|InvocationTargetException  e) {
                e.printStackTrace();
            }
        }
        return senders;

        
        //return new Sender[] { /*new ElasticSearch(context, outEndpoint, eventQueue) */};
    }

}
