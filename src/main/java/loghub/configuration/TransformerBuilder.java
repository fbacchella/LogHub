package loghub.configuration;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import loghub.Event;
import loghub.Transformer;

public class TransformerBuilder {

    private TransformerBuilder() {
    }
    
    public static Transformer[] create(String className, Map<String, Event> eventQueue, Map<String, String> properties) {
        
        try {
            @SuppressWarnings("unchecked")
            Class<Transformer> transClass = (Class<Transformer>) TransformerBuilder.class.getClassLoader().loadClass(className);
            //System.out.println(BeansManager.getBeanPropertiesMap(transClass, Transformer.class));
            Constructor<Transformer> constr = transClass.getConstructor(Map.class);
            String countThreadString = properties.remove("threads");
            int countThread;
            if(countThreadString == null) {
                countThread = 1;
            }
            else {
                countThread = BeansManager.ConstructFromString(Integer.class, countThreadString);
            }
            Transformer[] transformers = new Transformer[countThread];
            
            for(int i =0 ; i < countThread; i++) {
                transformers[i] = constr.newInstance(eventQueue);
                transformers[i].setName(className + "-" + i);
                for(Map.Entry<String, String> e: properties.entrySet()) {
                    BeansManager.beanSetter(transformers[i], e.getKey(), e.getValue());
                }
            }
            return transformers;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InstantiationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

}
