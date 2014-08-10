package loghub;

import java.lang.reflect.Constructor;
import java.security.Permission;
import java.security.Permissions;
import java.security.SecurityPermission;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.MBeanPermission;

import loghub.configuration.Configuration;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;

public class Start extends Thread {

    static private final Permissions allowed = new Permissions();

    static {
        //        Map<String, ObjectName> objNameMap = new HashMap<String, ObjectName>();
        try {
            //            for(String onameString: new String[] {"jrds:type=agent", "java.lang:type=Runtime"}) {
            //                ObjectName on = new ObjectName(onameString);
            //                objNameMap.put(onameString, on);
            //            }

            Object[][] args = new Object[][]{
                    //                    new Object[] {"-#-[-]", "getClassLoaderRepository"},
                    //                    new Object[] {"jrds.agent.RProbeJMXImpl", null, objNameMap.get("jrds:type=agent"), "*"},
                    //                    new Object[] {"jrds.agent.RProbeJMXImpl", "Uptime", objNameMap.get("jrds:type=agent"), "*"},
                    //                    new Object[] {"jrds.agent.RProbeJMXImpl", "query", objNameMap.get("jrds:type=agent"), "*"},
                    //                    new Object[] {"jrds.agent.RProbeJMXImpl", "prepare", objNameMap.get("jrds:type=agent"), "*"},
                    //                    new Object[] {"sun.management.RuntimeImpl", "Uptime", objNameMap.get("java.lang:type=Runtime"), "getAttribute"},
            };
            Map<Integer, Constructor<MBeanPermission>> constructsmap = new HashMap<Integer, Constructor<MBeanPermission>>();

            @SuppressWarnings("unchecked")
            Constructor<MBeanPermission>[] mbpermConstructors = (Constructor<MBeanPermission>[]) MBeanPermission.class.getConstructors();
            for(Constructor<MBeanPermission> c: mbpermConstructors ) {
                constructsmap.put(c.getGenericParameterTypes().length, c);
            }
            for(Object[] arg: args) {
                MBeanPermission aperm = constructsmap.get(arg.length).newInstance(arg);
                allowed.add(aperm);
            }
            Map<Class<?>, String[]> permByName = new HashMap<Class<?>, String[]>();
            permByName.put(RuntimePermission.class, new String[] {
                //                "exitVM.0",
                //                "getFileSystemAttributes", 
                //                "readFileDescriptor", "writeFileDescriptor", //Don't forget, network sockets are file descriptors
                //                "modifyThreadGroup", "modifyThread", //Needed by termination of the VM
                //                "setContextClassLoader", "getClassLoader", "createClassLoader",
                //                "sun.rmi.runtime.RuntimeUtil.getInstance", "sun.misc.Perf.getPerf", "reflectionFactoryAccess", "loadLibrary.rmi",
                //                "accessDeclaredMembers", "fileSystemProvider", "getProtectionDomain",
                //                "accessClassInPackage.sun.util.resources", "accessClassInPackage.sun.instrument", "accessClassInPackage.sun.management", "accessClassInPackage.sun.management.resources",
                //                "accessClassInPackage.sun.util.logging.resources", "accessClassInPackage.sun.text.resources", "accessClassInPackage.com.sun.jmx.remote.internal",
                //                "accessClassInPackage.sun.security.provider", "accessClassInPackage.com.sun.jmx.remote.protocol.jmxmp", "accessClassInPackage.sun.reflect", "accessClassInPackage.sun.reflect.misc",

            });
            permByName.put(SecurityPermission.class, new String[] {
                //                "getPolicy", "getProperty.networkaddress.cache.ttl", "getProperty.networkaddress.cache.negative.ttl", 
                //                "getProperty.security.provider", "getProperty.securerandom.source", "putProviderProperty.SUN", 
                //                "getProperty.security.provider.1", "getProperty.security.provider.2", "getProperty.security.provider.3", "getProperty.security.provider.4",
                //                "getProperty.security.provider.5", "getProperty.security.provider.6", "getProperty.security.provider.7", "getProperty.security.provider.8",
                //                "getProperty.security.provider.9", "getProperty.security.provider.10", "getProperty.security.provider.11"
            });
            for(Map.Entry<Class<?>, String[]> e: permByName.entrySet()) {
                @SuppressWarnings("unchecked")
                Constructor<Permission> c = (Constructor<Permission>) e.getKey().getConstructor(String.class);
                for(String permName: e.getValue()) {
                    Permission newPerm;
                    newPerm = c.newInstance(permName);
                    allowed.add(newPerm);
                }
            }

            String[][] permArgs = new String[][] {
                    //                    new String[] { "java.util.logging.LoggingPermission", "control", "" },
                    //                    new String[] { "java.net.NetPermission", "getProxySelector"},
                    //                    new String[] { "java.net.NetPermission", "specifyStreamHandler"},
                    //                    new String[] { "javax.management.MBeanServerPermission", "*"},
                    //                    new String[] { "java.lang.management.ManagementPermission", "monitor"},
                    //                    new String[] { "java.lang.reflect.ReflectPermission", "suppressAccessChecks"},
                    //                    new String[] { "java.io.SerializablePermission", "enableSubstitution"},
                    //                    new String[] { "java.io.FilePermission", "<<ALL FILES>>", "read"},
                    //                    new String[] { "java.util.PropertyPermission", "java.home", "read"},
                    //                    new String[] { "java.util.PropertyPermission", "java.rmi.server.*", "read"},
                    //                    new String[] { "java.util.PropertyPermission", "java.security.egd", "read"},
                    //                    new String[] { "java.util.PropertyPermission", "socksProxyHost", "read"},
                    //                    new String[] { "java.util.PropertyPermission", "jdk.logging.*", "read"},
                    //                    new String[] { "java.util.PropertyPermission", "sun.boot.class.path", "read"},
                    //                    new String[] { "java.util.PropertyPermission", "sun.io.serialization.extendedDebugInfo", "read"},
                    //                    new String[] { "java.util.PropertyPermission", "sun.net.maxDatagramSockets", "read"},
                    //                    new String[] { "java.util.PropertyPermission", "sun.rmi.*", "read"},
                    //                    new String[] { "java.util.PropertyPermission", "sun.timezone.*", "read"},
                    //                    new String[] { "java.util.PropertyPermission", "sun.util.logging.*", "read"},
                    //                    new String[] { "java.util.PropertyPermission", "com.sun.jmx.remote.bug.compatible", "read"},
                    //                    new String[] { "java.util.PropertyPermission", "os.arch", "read"},
                    //                    new String[] { "java.util.PropertyPermission", "user.language.format", "read"},
                    //                    new String[] { "java.util.PropertyPermission", "user.script.format", "read"},
                    //                    new String[] { "java.util.PropertyPermission", "user.country.format", "read"},
                    //                    new String[] { "java.util.PropertyPermission", "line.separator", "read"},
                    //                    new String[] { "java.util.PropertyPermission", "user.*", "read"},
                    //                    new String[] { "java.net.SocketPermission", "*", "accept,connect,listen,resolve"},
            };
            Class<?>[][] typeVector = new Class[][]{
                    new Class[] { String.class },
                    new Class[] { String.class, String.class },                    
            };
            for(String[] a: permArgs) {
                String className = a[0];
                String[] argVector = Arrays.copyOfRange(a, 1, a.length);
                Class<?> cl = Start.class.getClassLoader().loadClass(className);
                Constructor<?> c = cl.getConstructor( typeVector[argVector.length - 1]);
                Permission newPerm = (Permission) c.newInstance((Object[])argVector);
                allowed.add(newPerm);
            }
            allowed.setReadOnly();
        } catch (Exception e) {
            throw new RuntimeException("Permission initialization failed: " + e.getMessage(), e);
        }
    }

    static public void main(String[] args) {
        //Initialization done, set the security manager
        String withSecurity = "false";
        if (System.getSecurityManager() == null && Boolean.parseBoolean(withSecurity))
            System.setSecurityManager ( getSecurityManager() );

        //Make it wait on himself to wait forever
        try {
            Thread me = new Start();
            me.setName("LogHub");
            me.start();
            me.join();
        } catch (InterruptedException e) {
        }
    }

    public void run() {
        Context context = ZMQ.context(1);
        Map<byte[], Event> eventQueue = new ConcurrentHashMap<>();
        
        Configuration conf = new Configuration(eventQueue, context);
        
        conf.parse("conf/conf.yaml");

        List<PipeStep[]> pipe = conf.getTransformersPipe();
        PipeStream mainPipe = new PipeStream(eventQueue, context, "", pipe);

        for(Sender s: conf.getSenders(mainPipe.getOutEndpoint())) {
            s.start();
        }
        
        for(Receiver r: conf.getReceivers(mainPipe.getInEndpoint())) {
            r.start();
        }
        
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        context.term();
    }

    static final private SecurityManager getSecurityManager() {
        return new SecurityManager() {
            public void checkPermission(Permission perm) {
                if(allowed.implies(perm)) {
                    return;
                }
                try {
                    super.checkPermission(perm);
                } catch (Exception e) {
                    System.out.println(perm);
                }
            }
        };    
    }

}
