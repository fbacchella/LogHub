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

import loghub.Codec;
import loghub.Event;
import loghub.PipeStep;
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

import loghub.codec.StringCodec;

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
        @Override
        public String toString() {
            return className + beans;
        }
    }

    private final Map<byte[], Event> eventQueue;
    private final Context context;
    private final Yaml yaml = new Yaml();
    private final Map<String, List<ObjectInfo>> slots = new HashMap<>();

    public Configuration(Map<byte[], Event> eventQueue, Context context) {
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
//        try {
//            Object o;
//            o = yaml.load(new FileReader("conf/newconf.yaml"));
//            System.out.println(o);
//        } catch (FileNotFoundException e2) {
//            // TODO Auto-generated catch block
//            e2.printStackTrace();
//        }
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

    private <T> T createObject(ObjectInfo oi) {
        try {
            String className = oi.className;
            @SuppressWarnings("unchecked")
            Class<T> cl = (Class<T>) getClass().getClassLoader().loadClass(className);
            Constructor<T> c = cl.getConstructor();
            T t = c.newInstance();
            for(Map.Entry<String, String> bean: oi.beans.entrySet()) {
                BeansManager.beanSetter(t, bean.getKey(), bean.getValue());
            }
            return t;
        } catch (ClassNotFoundException|NoSuchMethodException|SecurityException|InstantiationException|IllegalAccessException|IllegalArgumentException|InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }

    public List<PipeStep[]> getTransformersPipe() {
        List<ObjectInfo> transformers = slots.get(PIPE_SLOT);
        List<PipeStep[]> pipe = new ArrayList<>();
        int rank = 0;
        int threads = -1;
        PipeStep[] step = null;
        for(ObjectInfo oi: transformers) {
            Transformer t = createObject(oi);
            if(t.getThreads() != threads) {
                threads = t.getThreads();
                step = new PipeStep[threads];
                pipe.add(step);
                rank++;
                for(int i=0; i < threads ; i++) {
                    step[i] = new PipeStep(rank, i + 1);
                }
            }
            for(int i = 0; i < threads ; i++) {
                step[i].addTransformer(t);
            }
        }
        return pipe;
    }

    public Receiver[] getReceivers(String inEndpoint) {
        List<ObjectInfo> descriptions = slots.get(IN_SLOT);
        Receiver[] receivers = new Receiver[descriptions.size()];
        int i = 0;
        Codec defaultCodec = new StringCodec();
        for(ObjectInfo oi: descriptions) {
            try {
                String className = oi.className;
                @SuppressWarnings("unchecked")
                Class<Receiver> cl = (Class<Receiver>) getClass().getClassLoader().loadClass(className);
                Constructor<Receiver> c = cl.getConstructor(Context.class, String.class, Map.class);
                Receiver r = c.newInstance(context, inEndpoint, eventQueue);
                // Let's build the codec
                Codec codec = defaultCodec;
                if(oi.beans.containsKey("codec")) {
                    String codecName = oi.beans.remove("codec");
                    codec = createObject(new ObjectInfo(codecName, new HashMap<String, String>(0)));          
                }
                r.setCodec(codec);
                for(Map.Entry<String, String> bean: oi.beans.entrySet()) {
                    BeansManager.beanSetter(r, bean.getKey(), bean.getValue());
                }
                receivers[i++] = r;
            } catch (ClassNotFoundException|NoSuchMethodException|SecurityException|InstantiationException|IllegalAccessException|IllegalArgumentException|InvocationTargetException  e) {
                e.printStackTrace();
            }
        }
        System.out.println(yaml.dump(receivers));
        return receivers;
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
        System.out.println(yaml.dump(senders));

        return senders;

    }

}
