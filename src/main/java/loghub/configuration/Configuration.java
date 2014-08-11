package loghub.configuration;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import loghub.Codec;
import loghub.Event;
import loghub.PipeStep;
import loghub.Receiver;
import loghub.Sender;
import loghub.Transformer;
import loghub.configuration.StackConf.ParseException;

import org.zeromq.ZMQ.Context;

public class Configuration {

    static private final String IN_SLOT = "in";
    static private final String OUT_SLOT = "out";
    static private final String PIPE_SLOT = "pipe";

    private final Map<byte[], Event> eventQueue;
    private final Context context;
    private final Map<String, List<Object>> slots = new HashMap<>();

    public Configuration(Map<byte[], Event> eventQueue, Context context) {
        this.eventQueue = eventQueue;
        this.context = context;

        slots.put(IN_SLOT, new ArrayList<Object>());
        slots.put(OUT_SLOT, new ArrayList<Object>());
        slots.put(PIPE_SLOT, new ArrayList<Object>());
    }

    public void parse(String fileName) {
        try {
            Reader r = new FileReader(fileName);
            StackConf stack = new StackConf(r);
            StackConf.State next;
            stack.step(StackConf.State.STREAM);
            stack.step(StackConf.State.DOCUMENT);
            stack.step(StackConf.State.SLOTS);
            while((next = stack.step(StackConf.State.SLOT_NAME, StackConf.State.SLOTS)) == StackConf.State.SLOT_NAME) {
                String slotName = stack.getValue();
                if(! slots.containsKey(slotName)) {
                    throw new ParseException("illegal slot name " +  slotName, stack.getEvent());
                }
                next = stack.step(StackConf.State.SLOT_OBJECT_SEQUENCE, StackConf.State.SLOT_OBJECT);
                switch(next) {
                case SLOT_OBJECT_SEQUENCE:
                    while((next = stack.step(StackConf.State.SLOT_OBJECT, StackConf.State.SLOT_OBJECT_SEQUENCE)) == StackConf.State.SLOT_OBJECT) {
                        Object o = doObject(stack);
                        slots.get(slotName).add(o);
                    }
                    break;
                case SLOT_OBJECT:
                    Object o = doObject(stack);
                    slots.get(slotName).add(o);
                    break;
                default:
                    throw new ParseException("unreachable code", stack.getEvent());
                }
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File " + fileName + "not found", e);
        } catch (ParseException e) {
            throw new RuntimeException(e.getMessage() + e.event.getStartMark(), e);
        }
    }

    private Object doObject(StackConf stack) throws ParseException {
        Object o = stack.doObject();
        Set<String> beans = BeansManager.getBeans(o.getClass());
        for(StackConf.BeanInfo i: stack.getBeans()) {
            if(! beans.contains(i.name)) {
                throw new ParseException("Unknown bean " + i.name, stack.getEvent());
            }
            try {
                if("codec".equals(i.name)) {
                    Object argument = doObject(stack);
                    ((Receiver) o).setCodec((Codec)argument);
                } else {
                    BeansManager.beanSetter(o, i.name, i.value);                    
                }
            } catch (InvocationTargetException e) {
                throw new ParseException("Invalid bean " + i, stack.getEvent(), e);
            }
        }
        return o;
    }

    public List<PipeStep[]> getTransformersPipe() {
        List<Object> transformers = slots.get(PIPE_SLOT);
        List<PipeStep[]> pipe = new ArrayList<>();
        int rank = 0;
        int threads = -1;
        PipeStep[] step = null;
        for(Object o: transformers) {
            Transformer t = (Transformer) o;
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
        List<Object> descriptions = slots.get(IN_SLOT);
        Receiver[] receivers = new Receiver[descriptions.size()];
        int i = 0;
        for(Object o: descriptions) {
            Receiver r = (Receiver) o;
            r.configure(context, inEndpoint, eventQueue);
            receivers[i++] = r;
        }
        return receivers;
    }

    public Sender[] getSenders(String outEndpoint) {
        List<Object> descriptions = slots.get(OUT_SLOT);
        Sender[] senders = new Sender[descriptions.size()];
        int i = 0;
        for(Object oi: descriptions) {
            Sender r = (Sender) oi;
            r.configure(context, outEndpoint, eventQueue);
            senders[i++] = r;
        }

        return senders;

    }

}
