package loghub;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Timer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator;
import com.axibase.date.DatetimeProcessor;
import com.axibase.date.PatternResolver;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import loghub.configuration.ConfigException;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.configuration.SecretsHandler;
import loghub.configuration.TestEventProcessing;
import loghub.events.Event;
import loghub.metrics.JmxService;
import loghub.metrics.Stats;
import loghub.processors.FieldsProcessor;
import loghub.receivers.Receiver;
import loghub.security.JWTHandler;
import loghub.senders.Sender;
import loghub.sources.Source;
import lombok.ToString;

public class Start {

    /**
     * Used to define custom exit code, start at 10 because I don't know what exit code are reserver by the JVM.
     * For example, ExitOnOutOfMemoryError return 3.
     * 
     * @author Fabrice Bacchella
     *
     */
    private static class ExitCode {
        private static final int OK = 0;
        private static final int INVALIDCONFIGURATION = 10;
        private static final int FAILEDSTART = 11;
        private static final int FAILEDSTARTCRITICAL = 12;
        private static final int OPERATIONFAILED = 13;
        private static final int INVALIDARGUMENTS = 14;
        private static final int CRITICALFAILURE = 99;
    }

    private static final String SECRETS_CMD = "secrets";
    private static final String JWT_CMD = "jwt";

    // Prevent launching fatal shutdown many times
    // Reset in constructor
    private static boolean catchedcritical = false;

    // Keep a reference to allow external shutdown of the processing.
    // Needed by some tests
    private static Thread shutdownAction;

    // Memorize canexit, needed when loghub.Start is tested
    private static boolean testscanexit;

    /**
     * To be called when a fatal exception is detected. It will generate a shutdown with a failure exit code.<br>
     * To be called when a thread catch any unhandled exception, or anytime when a critical exception is catched
     * @param t the uncatched exception
     */
    public static synchronized void fatalException(Throwable t) {
        // No emergency exist on InterruptedException, it's already a controlled shutdown
        if (! (t instanceof InterruptedException) && ! catchedcritical) {
            System.err.println("Caught a fatal exception");
            t.printStackTrace();
            shutdown();
            if (testscanexit) {
                System.exit(ExitCode.CRITICALFAILURE);
            }
        }
    }

    static synchronized void shutdown() {
        if (shutdownAction != null) {
            shutdownAction.run();
            Runtime.getRuntime().removeShutdownHook(shutdownAction);
            shutdownAction = null;
        }
    }

    @Parameters(commandNames={SECRETS_CMD})
    @ToString
    static class CommandPassword {
        // Secret sources
        @Parameter(names = {"--secret", "-S"}, description = "Secret")
        String secretValue = null;
        @Parameter(names = {"--file", "-f"}, description = "Password file")
        String fromFile = null;
        @Parameter(names = {"--console", "-c"}, description = "Read from console")
        boolean fromConsole = false;
        @Parameter(names = {"--stdin", "-i"}, description = "Read from stdin")
        boolean fromStdin = false;

        // Identification elements
        @Parameter(names = {"--alias", "-a"}, description = "Secret entry alias")
        String alias = null;
        @Parameter(names = {"--store", "-s"}, description = "The store file", required = true)
        String storeFile = null;

        // Actions
        @Parameter(names = {"--add"}, description = "Add a secret")
        boolean add = false;
        @Parameter(names = {"--del"}, description = "Delete a secret")
        boolean delete = false;
        @Parameter(names = {"--list"}, description = "List secrets")
        boolean list = false;
        @Parameter(names = {"--create"}, description = "Create te store file")
        boolean create = false;

        void process() throws IOException {
            if ((add ? 1 : 0) + (delete ? 1 : 0) + (list ? 1 : 0) + (create ? 1 : 0) != 1) {
                throw new IllegalStateException("A single action is required");
            }
            if ((fromConsole ? 1 : 0) + (fromStdin ? 1 : 0) + (secretValue != null ? 1 : 0) + (fromFile != null ? 1 : 0) > 1) {
                throw new IllegalStateException("Multiple secret sources given, pick one");
            }
            if ((fromConsole ? 1 : 0) + (fromStdin ? 1 : 0) + (secretValue != null ? 1 : 0) + (fromFile != null ? 1 : 0) == 0) {
                // The default input is console
                fromConsole = true;
            }
            if (add) {
                try (SecretsHandler sh = SecretsHandler.load(storeFile)) {
                    sh.add(alias, readSecret());
                }
            } else if (delete) {
                try (SecretsHandler sh = SecretsHandler.load(storeFile)) {
                    sh.delete(alias);
                }
            } else if (list) {
                try (SecretsHandler sh = SecretsHandler.load(storeFile)) {
                    sh.list().map(Map.Entry::getKey).forEach(System.out::println);
                }
            } else if (create) {
                try (SecretsHandler sh = SecretsHandler.create(storeFile)) {
                    // Nothing to do
                }
            }
        }

        private byte[] readSecret() throws IOException {
            byte[] secret;
            if (fromConsole) {
                secret = new String(System.console().readPassword()).getBytes(StandardCharsets.UTF_8);
            } else if (secretValue != null) {
                secret = secretValue.getBytes(StandardCharsets.UTF_8);
            } else if (fromStdin) {
                ByteBuf buffer = Unpooled.buffer();
                byte[] readbuffer = new byte[256];
                while (System.in.read(readbuffer) > 0) {
                    buffer.writeBytes(readbuffer);
                }
                secret = new byte[buffer.readableBytes()];
                buffer.readBytes(secret);
            } else if (fromFile != null) {
                secret = Files.readAllBytes(Paths.get(fromFile));
            } else {
                throw new IllegalStateException("No secret source defined");
            }
            return secret;
        }
    }

    @Parameters(commandNames={JWT_CMD})
    @ToString
    static class CommandJwt {
        public static class ClaimConverter implements IStringConverter<AbstractMap.SimpleImmutableEntry<String, String>> {
            @Override
            public AbstractMap.SimpleImmutableEntry<String, String> convert(String value) {
                String[] s = value.split("=");
                if (s.length != 2) {
                    System.err.println("bad claim: " + value);
                    System.exit(ExitCode.INVALIDARGUMENTS);
                }
                return new AbstractMap.SimpleImmutableEntry(s[0], s[1]);
            }
        }

        @Parameter(names = {"--gen"}, description = "Generate a JWT token")
        private boolean generate = false;

        @Parameter(names = {"--subject", "-s"}, description = "Generate a JWT token")
        private String subject = null;

        @Parameter(names = {"--validity", "-v"}, description = "The jwt token validity in days")
        private long validity = -1;

        @Parameter(names = {"--claim", "-c"}, description = "Add a claim", converter = ClaimConverter.class)
        private List<AbstractMap.SimpleImmutableEntry<String, String>> claims = new ArrayList<>();

        @Parameter(names = {"--sign"}, description = "Sign a JWT token")
        private boolean sign = false;

        @Parameter(names = {"--signfile", "-f"}, description = "The jwt token to sign")
        private String signfile = null;

        void process(JWTHandler handler) {
            if (sign) {
                sign(signfile, handler);
            } else if (generate) {
                generate(subject, handler);
            }
        }

        private void generate(String subject, JWTHandler handler) {
            JWTCreator.Builder builder = JWT.create()
                    .withSubject(subject)
                    .withIssuedAt(new Date());
            for (Map.Entry<String, String> claim: claims) {
                builder.withClaim(claim.getKey(), claim.getValue());
            }
            if (validity > 0) {
                Instant end = ZonedDateTime.now(ZoneOffset.UTC).plus(validity, ChronoUnit.DAYS).toInstant();
                builder.withExpiresAt(Date.from(end));
            }
            System.out.println(handler.getToken(builder));
            System.exit(ExitCode.OK);
        }

        private void sign(String signfile, JWTHandler handler) {
            if (signfile == null) {
                System.err.println("No JWT payload");
            }
            try {
                byte[] buffer = Files.readAllBytes(Paths.get(signfile));
                String token = handler.sign(new String(buffer, StandardCharsets.UTF_8));
                System.out.println(token);
                System.exit(ExitCode.OK);
            } catch (IOException e) {
                System.err.println("Can't read JWT payload: " + Helpers.resolveThrowableException(e));
                logger.catching(e);
            }
        }
    }

    @Parameter(names = {"--configfile", "-c"}, description = "File")
    String configFile = null;

    @Parameter(names = {"--help", "-h"}, help = true)
    private boolean help;

    @Parameter(names = {"--test", "-t"}, description = "Test mode")
    private boolean test = false;

    @Parameter(names = {"--stats", "-s"}, description = "Dump stats on exit")
    private boolean dumpstats = false;

    // Default to false, set to true in the main method, because it should not be able to stop during tests
    @Parameter(names = "--canexit", description = "Prevent call to System.exit(), for JUnit tests only", hidden = true)
    private boolean canexit = false;

    @Parameter(names = {"--testprocessor", "-p"}, description = "A field processor to test")
    private String testedprocessor = null;

    @Parameter(names = {"--timepattern"}, description = "A time pattern to test")
    private String timepattern = null;

    String pipeLineTest = null;
    int exitcode = ExitCode.OK;

    // To be executed before LogManager.getLogger() to ensure that log4j2 will use the basis context selector
    // Not the smart one for web app.
    static {
        System.setProperty("Log4jContextSelector", "org.apache.logging.log4j.core.selector.BasicContextSelector");
        System.setProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager");
    }

    private static final Logger logger = LogManager.getLogger();

    public static void main(final String[] args) {
        Start main = new Start();
        main.canexit = true;
        CommandPassword passwd = new CommandPassword();
        CommandJwt jwt = new CommandJwt();
        JCommander jcom = JCommander
                        .newBuilder()
                        .addObject(main)
                        .addCommand(passwd)
                        .addCommand(jwt)
                        .acceptUnknownOptions(true)
                        .build();

        try {
            jcom.parse(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            System.exit(ExitCode.INVALIDARGUMENTS);
        }
        if (main.help) {
            jcom.usage();
            System.exit(ExitCode.OK);
        }
        testscanexit = main.canexit;
        if (SECRETS_CMD.equals(jcom.getParsedCommand())) {
            try {
                passwd.process();
            } catch (IOException | IllegalArgumentException ex) {
                System.err.println("Secret store operation failed: " + Helpers.resolveThrowableException(ex));
                System.exit(ExitCode.OPERATIONFAILED);
            } catch (IllegalStateException ex) {
                System.err.println("Secret store state broken: " + Helpers.resolveThrowableException(ex));
                ex.printStackTrace();
                System.exit(ExitCode.OPERATIONFAILED);
            }
        } else if (JWT_CMD.equals(jcom.getParsedCommand())) {
            if (main.configFile == null) {
                System.err.println("No configuration file given");
                System.exit(ExitCode.INVALIDCONFIGURATION);
            }
            try {
                Properties props = Configuration.parse(main.configFile);
                jwt.process(props.jwtHandler);
            } catch (IOException | IllegalArgumentException ex) {
                System.err.println("JWT operation failed: " + Helpers.resolveThrowableException(ex));
                System.exit(ExitCode.OPERATIONFAILED);
            } catch (IllegalStateException ex) {
                System.err.println("JWT state broken: " + Helpers.resolveThrowableException(ex));
                System.exit(ExitCode.OPERATIONFAILED);
            }
        } else if (main.timepattern != null) {
            DatetimeProcessor tested = PatternResolver.createNewFormatter(main.timepattern);
            for (String date: jcom.getUnknownOptions()) {
                try {
                    System.out.format("%s -> %s%n", date, tested.parse(date));
                } catch (IllegalArgumentException | DateTimeParseException ex) {
                    System.out.format("%s failed%n", date);
                }
            }
        } else {
            main.configure();
        }
    }

    Start() {
        // Reset catched criticaly, Start can be used many time in tests.
        catchedcritical = false;
        shutdownAction = null;
    }

    private void configure() {
        if (testedprocessor != null) {
            test = true;
            dumpstats = false;
        }
        if (pipeLineTest != null) {
            TestEventProcessing.check(pipeLineTest, configFile);
        }

        try {
            if (configFile == null) {
                System.err.println("No configuration file given");
                exitcode = ExitCode.INVALIDCONFIGURATION;
            } else {
                Properties props = Configuration.parse(configFile);
                if (!test) {
                    launch(props);
                    logger.warn("LogHub started");
                } else if (testedprocessor != null) {
                    testProcessor(props, testedprocessor);
                }
            }
        } catch (ConfigException e) {
            System.err.format("Error in %s: %s%n", e.getLocation(), e.getMessage());
            exitcode = ExitCode.INVALIDCONFIGURATION;
        } catch (IOException e) {
            System.err.format("Can't read configuration file %s: %s%n", configFile, Helpers.resolveThrowableException(e));
            exitcode = ExitCode.INVALIDCONFIGURATION;
        } catch (IllegalStateException e) {
            // Thrown by launch when a component failed to start, details are in the logs
            System.err.format("Failed to start loghub: %s%n", Helpers.resolveThrowableException(e));
            exitcode = ExitCode.FAILEDSTART;
        } catch (Throwable e) {
            System.err.format("Failed to start loghub for an unhandled cause: %s%n", Helpers.resolveThrowableException(e));
            e.printStackTrace();
            exitcode = ExitCode.FAILEDSTARTCRITICAL;
        }
        if (canexit && exitcode != 0) {
            System.exit(exitcode);
        } else if (exitcode != 0) {
            throw new RuntimeException("Exit code would be " + exitcode);
        }
    }


    private void testProcessor(Properties props, String testedprocessor2) {
        Processor p = props.identifiedProcessors.get(testedprocessor2);
        if (p == null) {
            System.err.println("Unidentified processor");
        } else if (! (p instanceof FieldsProcessor)) {
            System.err.println("Not a field processor");
        } else {
            p.configure(props);
            FieldsProcessor fp = (FieldsProcessor) p;
            Event ev = props.eventsFactory.newEvent();
            try {
                new BufferedReader(new InputStreamReader(System.in, "UTF-8")).lines().forEach( i -> {
                    try {
                        ev.put("message", i);
                        fp.fieldFunction(ev, i);
                        System.out.format("%s -> %s%n", i, ev);
                    } catch (ProcessorException e) {
                        System.err.println("Processing failed:" + e.getMessage());
                    }
                });
            } catch (UnsupportedEncodingException e) {
            }
        }
    }

    public void launch(Properties props) throws ConfigException, IOException {
        try {
            JmxService.start(props.jmxServiceConfiguration);
        } catch (IOException e) {
            logger.error("JMX start failed: {}", Helpers.resolveThrowableException(e));
            logger.catching(Level.DEBUG, e);
            throw new IllegalStateException("JMX start failed: " + Helpers.resolveThrowableException(e));
        }

        if (props.dashboard != null) {
            try {
                props.dashboard.start();
            } catch (IllegalArgumentException e) {
                logger.error("Unable to start HTTP dashboard: {}", Helpers.resolveThrowableException(e));
                logger.catching(Level.DEBUG, e);
                throw new IllegalStateException("Unable to start HTTP dashboard: " + Helpers.resolveThrowableException(e));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while starting dashboard");
            }
        }

        // Used to remember if configuration process succeded
        // So ensure that the whole configuration is tested instead needed
        // many tests
        boolean failed = false;

        long starttime = System.nanoTime();

        for (Map.Entry<String, Source> s: props.sources.entrySet()) {
            if (! s.getValue().configure(props)) {
                logger.error("failed to start source {}", s.getKey());
                failed = true;
            }
        }

        Helpers.parallelStartProcessor(props);

        for (Sender s: props.senders) {
            s.setUncaughtExceptionHandler(ThreadBuilder.DEFAULTUNCAUGHTEXCEPTIONHANDLER);
            try {
                if (s.configure(props)) {
                    s.start();
                } else {
                    logger.error("failed to configure sender {}", s.getName());
                    failed = true;
                }
            } catch (Throwable e) {
                if (Helpers.isFatal(e)) {
                    throw e;
                } else {
                    logger.error("failed to start sender {}", s.getClass().getName());
                    failed = true;
                }
            }
        }

        if (! failed) {
            for (EventsProcessor ep: props.eventsprocessors) {
                ep.setUncaughtExceptionHandler(ThreadBuilder.DEFAULTUNCAUGHTEXCEPTIONHANDLER);
                ep.start();
            }
            Helpers.waitAllThreads(props.eventsprocessors.stream());
        }

        for (Receiver r: props.receivers) {
            r.setUncaughtExceptionHandler(ThreadBuilder.DEFAULTUNCAUGHTEXCEPTIONHANDLER);
            try {
                if (r.configure(props)) {
                    // Only start if not failed. Avoid swallowing events and latter discard them
                    if (! failed) {
                        r.start();
                    }
                } else {
                    logger.error("failed to configure receiver {}", r.getName());
                    failed = true;
                }
            } catch (Throwable e) {
                if (Helpers.isFatal(e)) {
                    throw e;
                } else {
                    logger.error("failed to start receiver {}", r.getClass().getName());
                    failed = true;
                }
            }
        }

        if (failed) {
            throw new IllegalStateException("Failed to start a component, see logs for more details");
        }

        // The shutdown runnable needs to be able to run on degraded JVM,
        // so reduced allocation and executed code inside
        Receiver[] receivers = props.receivers.stream().toArray(Receiver[]::new);
        EventsProcessor[] eventProcessors = props.eventsprocessors.stream().toArray(EventsProcessor[]::new);
        Sender[] senders = props.senders.stream().toArray(Sender[]::new);
        Timer loghubtimer = props.timer;
        Runnable shutdown = () -> {
            loghubtimer.cancel();
            for (int i = 0 ; i < receivers.length ; i++) {
                Receiver r = receivers[i];
                r.stopReceiving();
                receivers[i] = null;
            }
            for (int i = 0 ; i < eventProcessors.length ; i++) {
                EventsProcessor ep = eventProcessors[i];
                ep.stopProcessing();
                eventProcessors[i] = null;
            }
            for (int i = 0 ; i < senders.length ; i++) {
                Sender s = senders[i];
                s.stopSending();
                senders[i] = null;
            }
            props.terminate();
            JmxService.stop();
            if (dumpstats) {
                long endtime = System.nanoTime();
                double runtime = ((endtime - starttime)) / 1.0e9;
                System.out.format("Received: %.2f/s%n", Stats.getReceived() / runtime);
                System.out.format("Dropped: %.2f/s%n", Stats.getDropped() / runtime);
                System.out.format("Sent: %.2f/s%n", Stats.getSent() / runtime);
                System.out.format("Failures: %.2f/s%n", Stats.getFailed() / runtime);
                System.out.format("Exceptions: %.2f/s%n", Stats.getExceptionsCount() / runtime);
            }
        };

        shutdownAction = ThreadBuilder.get()
                                      .setDaemon(false) // not a daemon, so it will prevent stopping the JVM until finished
                                      .setTask(shutdown)
                                      .setName("StopEventsProcessing")
                                      .setShutdownHook(true)
                                      .setExceptionHandler(null)
                                      .build(false);

    }

    void setCanexit(boolean canexit) {
        this.canexit = canexit;
        Start.testscanexit = canexit;
    }

}
