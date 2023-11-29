package loghub.configuration;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;
import javax.security.auth.login.Configuration;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.IntStream;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.AbstractBuilder;
import loghub.Expression;
import loghub.Helpers;
import loghub.Lambda;
import loghub.NullOrMissingValue;
import loghub.Pipeline;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.RouteBaseListener;
import loghub.RouteParser;
import loghub.RouteParser.ArrayContext;
import loghub.RouteParser.BeanContext;
import loghub.RouteParser.BooleanLiteralContext;
import loghub.RouteParser.CharacterLiteralContext;
import loghub.RouteParser.DropContext;
import loghub.RouteParser.EtlContext;
import loghub.RouteParser.EventVariableContext;
import loghub.RouteParser.ExpressionContext;
import loghub.RouteParser.ExpressionsListContext;
import loghub.RouteParser.FinalpiperefContext;
import loghub.RouteParser.FireContext;
import loghub.RouteParser.FloatingPointLiteralContext;
import loghub.RouteParser.ForkpiperefContext;
import loghub.RouteParser.ForwardpiperefContext;
import loghub.RouteParser.InputContext;
import loghub.RouteParser.InputObjectlistContext;
import loghub.RouteParser.IntegerLiteralContext;
import loghub.RouteParser.LogContext;
import loghub.RouteParser.MapContext;
import loghub.RouteParser.MergeArgumentContext;
import loghub.RouteParser.MergeContext;
import loghub.RouteParser.NullLiteralContext;
import loghub.RouteParser.ObjectContext;
import loghub.RouteParser.OutputContext;
import loghub.RouteParser.OutputObjectlistContext;
import loghub.RouteParser.PathContext;
import loghub.RouteParser.PathElementContext;
import loghub.RouteParser.PipelineContext;
import loghub.RouteParser.PipenodeContext;
import loghub.RouteParser.PipenodeListContext;
import loghub.RouteParser.PiperefContext;
import loghub.RouteParser.PropertyContext;
import loghub.RouteParser.SecretContext;
import loghub.RouteParser.SourcedefContext;
import loghub.RouteParser.StringLiteralContext;
import loghub.RouteParser.TestContext;
import loghub.RouteParser.TestExpressionContext;
import loghub.RouteParser.VarPathContext;
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.processors.AnonymousSubPipeline;
import loghub.processors.Drop;
import loghub.processors.Etl;
import loghub.processors.FireEvent;
import loghub.processors.Forker;
import loghub.processors.Forwarder;
import loghub.processors.Log;
import loghub.processors.Mapper;
import loghub.processors.Merge;
import loghub.processors.NamedSubPipeline;
import loghub.processors.Test;
import loghub.processors.UnwrapEvent;
import loghub.processors.WrapEvent;
import loghub.receivers.Receiver;
import loghub.security.JWTHandler;
import loghub.senders.Sender;
import loghub.sources.Source;
import lombok.Builder;
import lombok.Data;

class ConfigListener extends RouteBaseListener {

    private static final Logger logger = LogManager.getLogger();

    private static final Class[] INJECTED_BEANS_CLASSES = new Class[]{
            SSLContext.class,
            javax.security.auth.login.Configuration.class,
            JWTHandler.class,
            ClassLoader.class,
            CacheManager.class,
            ConfigurationProperties.class,
    };

    private enum StackMarker {
        PATH,
        TEST,
        OBJECT_LIST,
        PIPE_NODE_LIST,
        ARRAY,
        EXPRESSION,
        EXPRESSION_LIST,
        FIRE,
        ETL,
        MAP
    }

    static final class Input {
        final List<ObjectWrapped<Receiver>> receiver;
        final String piperef;
        Input(List<ObjectWrapped<Receiver>>receiver, String piperef) {
            this.piperef = piperef;
            this.receiver = receiver;
        }
        @Override
        public String toString() {
            return "(" + receiver.toString() + " -> " + piperef + ")";
        }
    }

    static final class Output {
        final List<ObjectWrapped<Sender>> sender;
        final String piperef;
        Output(List<ObjectWrapped<Sender>>sender, String piperef) {
            this.piperef = piperef;
            this.sender = sender;
        }
        @Override
        public String toString() {
            return "(" + piperef + " -> " +  sender.toString() + ")";
        }
    }

    interface Pipenode {}

    static final class PipenodesList implements Pipenode {
        final List<ProcessorInstance> processors = new ArrayList<>();
        String nextPipelineName;
    }

    @Data
    static final class PipeRef implements Pipenode {
        String pipename;
    }

    @Data
    static final class PipeRefName implements Pipenode {
        final String piperef;
        private PipeRefName(String piperef) {
            this.piperef = piperef;
        }
    }

    static class ObjectWrapped<T> {
        final T wrapped;
        ObjectWrapped(T wrapped) {
            this.wrapped = wrapped;
        }
        @Override
        public String toString() {
            return "ObjectWrapped(" + wrapped + ")";
        }
    }

    static final class ProcessorInstance extends ObjectWrapped<Processor> implements Pipenode {
        ProcessorInstance(ObjectWrapped<Processor> wrapper) {
            super(wrapper.wrapped);
        }
        ProcessorInstance(Processor processor) {
            super(processor);
        }
        @Override
        public String toString() {
            return "ProcessorInstance$" + wrapped.getClass().getSimpleName() + "@" + System.identityHashCode(wrapped);
        }

    }

    static final class TypedStack extends ArrayDeque<Object> {
        public <T> T popTyped() {
            @SuppressWarnings("unchecked")
            T temp = (T) pop();
            return temp;
        }
        public <T> T peekTyped() {
            @SuppressWarnings("unchecked")
            T temp = (T) peek();
            return temp;
        }
    }

    final TypedStack stack = new TypedStack();

    final Map<String, PipenodesList> pipelines = new HashMap<>();
    final List<Input> inputs = new ArrayList<>();
    final List<Output> outputs = new ArrayList<>();
    final Map<String, AtomicReference<Source>> sources = new HashMap<>();
    final Set<String> outputPipelines = new HashSet<>();
    final SSLContext sslContext;
    final Configuration jaasConfig;
    final JWTHandler jwtHandler;

    private String currentPipeLineName = null;
    private int expressionDepth = 0;
    private final ParseTreeWalker walker = new ParseTreeWalker();
    private final java.util.Map<String, Pattern> patternCache = new HashMap<>();

    private final ClassLoader classLoader;
    private final SecretsHandler secrets;
    private final Map<String, String> lockedProperties;
    private final CacheManager cacheManager;
    private final BeansManager beansManager;
    final ConfigurationProperties properties;

    private Parser parser;
    private IntStream stream;

    @Builder
    private ConfigListener(ClassLoader classLoader, SecretsHandler secrets, Map<String, String> lockedProperties,
            SSLContext sslContext, javax.security.auth.login.Configuration jaasConfig, JWTHandler jwtHandler, CacheManager cacheManager,
            ConfigurationProperties properties) {
        this.classLoader = classLoader != null ? classLoader : ConfigListener.class.getClassLoader();
        this.secrets = secrets != null ? secrets : SecretsHandler.empty();
        this.lockedProperties = lockedProperties != null ? lockedProperties : new HashMap<>();
        this.beansManager = new BeansManager();
        this.sslContext = sslContext;
        this.jaasConfig = jaasConfig;
        this.jwtHandler = jwtHandler;
        this.cacheManager = cacheManager;
        this.properties = Optional.ofNullable(properties).filter(Objects::nonNull).orElseGet(ConfigurationProperties::new);
    }

    public void startWalk(ParserRuleContext config, CharStream stream, RouteParser parser) {
        this.stream = stream;
        this.parser = parser;
        walker.walk(this, config);
    }

    @Override
    public void enterPiperef(PiperefContext ctx) {
        stack.push(new PipeRefName(ctx.getText()));
    }

    private void pushLiteral(Object content) {
        stack.push(new ObjectWrapped<>(content));
    }

    @Override
    public void enterFloatingPointLiteral(FloatingPointLiteralContext ctx) {
        String content = ctx.FloatingPointLiteral().getText();
        pushLiteral(Double.valueOf(content));
    }

    @Override
    public void enterCharacterLiteral(CharacterLiteralContext ctx) {
        String content = ctx.CharacterLiteral().getText();
        pushLiteral(content.charAt(0));
    }

    @Override
    public void enterStringLiteral(StringLiteralContext ctx) {
        String content = ctx.StringLiteral().getText();
        pushLiteral(content);
    }

    @Override
    public void enterIntegerLiteral(IntegerLiteralContext ctx) {
        pushLiteral(resolveNumberLiteral(ctx));
    }

    @Override
    public void enterBooleanLiteral(BooleanLiteralContext ctx) {
        String content = ctx.getText();
        pushLiteral(Boolean.valueOf(content));
    }

    @Override
    public void enterNullLiteral(NullLiteralContext ctx) {
        pushLiteral(null);
    }

    
    @Override
    public void exitSecret(SecretContext ctx) {
        if (secrets == null) {
            throw new RecognitionException("No secrets source defined, but a secret used", parser, stream, ctx);
        } else {
            byte[] secret = secrets.get(ctx.id.getText());
            if (secret == null) {
                throw new RecognitionException("Unknown secret: " + ctx.id.getText(), parser, stream, ctx);
            } else if (ctx.SecretAttribute() == null || ! "blob".equals(ctx.SecretAttribute().getText())) {
                pushLiteral(new String(secret, StandardCharsets.UTF_8));
            } else {
                pushLiteral(secret);
            }
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void exitBean(BeanContext ctx) {
        String beanName;
        ObjectWrapped<Object> beanValue = stack.popTyped();
        if (ctx.fev != null) {
            stack.push(beanValue);
            beanName = ctx.bn.getText();
            VariablePath ev = convertEventVariable(ctx.fev);
            beanValue = new ObjectWrapped(ev);
        } else if (ctx.fsv != null) {
            beanName = ctx.bn.getText();
            Object value = VariablePath.parse(ctx.fsv.getText());
            beanValue = new ObjectWrapped(value);
        } else if (ctx.bn != null) {
            beanName = ctx.bn.getText();
        } else {
            beanName = ctx.beanName().getText();
        }
        doBean(beanName, beanValue, ctx);
    }

    @Override
    public void exitMergeArgument(MergeArgumentContext ctx) {
        String beanName = ctx.type.getText();
        ObjectWrapped<Object> beanValue = stack.popTyped();
        doBean(beanName, beanValue, ctx);
    }

    private void doBean(String beanName, ObjectWrapped<?> beanValue, ParserRuleContext ctx) {
        ObjectWrapped<Object> beanObject = stack.peekTyped();
        assert (beanName != null);
        assert (beanValue != null);
        try {
            beansManager.beanSetter(beanObject.wrapped, beanName, beanValue.wrapped);
        } catch (IntrospectionException | InvocationTargetException | UndeclaredThrowableException e) {
            throw new RecognitionException(Helpers.resolveThrowableException(e), parser, stream, ctx);
        }
    }

    @Override
    public void enterMerge(MergeContext ctx) {
        stack.push(new ObjectWrapped<>(new Merge()));
    }

    ObjectWrapped<Object> getObject(String qualifiedName, ParserRuleContext ctx) {
        try {
            logger.debug("Loading {} with {}", qualifiedName, classLoader);
            Class<?> objectClass = classLoader.loadClass(qualifiedName);
            AbstractBuilder<?> builder = AbstractBuilder.resolve(objectClass);
            return new ObjectWrapped<>(builder != null ? builder : objectClass.getConstructor().newInstance());
        } catch (ClassNotFoundException e) {
            throw new RecognitionException("Unknown class " + qualifiedName, parser, stream, ctx);
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | IllegalArgumentException | SecurityException e) {
            throw new RecognitionException("Unsuable class " + qualifiedName + ": " + Helpers.resolveThrowableException(e), parser, stream, ctx);
        } catch (InvocationTargetException e) {
            throw new RecognitionException("Unsuable class " + qualifiedName + ": " + Helpers.resolveThrowableException(e.getCause()), parser, stream, ctx);
        }
    }

    @Override
    public void enterObject(ObjectContext ctx) {
        String qualifiedName = ctx.QualifiedIdentifier().getText();
        stack.push(getObject(qualifiedName, ctx));
    }

    @Override
    public void exitObject(ObjectContext ctx) {
        ObjectWrapped<Object> wobject = stack.popTyped();
        if (wobject.wrapped instanceof AbstractBuilder) {
            AbstractBuilder<?> builder = (AbstractBuilder<?>) wobject.wrapped;
            // Check for some beans to inject
            for (Class<?> c: INJECTED_BEANS_CLASSES) {
                beansManager.getBeanByType(builder, c).ifPresent(m -> {
                    try {
                        Object value;
                        if (c == SSLContext.class) {
                            value = this.sslContext;
                        } else if (c == Configuration.class) {
                            value = this.jaasConfig;
                        } else if (c == JWTHandler.class) {
                            value = this.jwtHandler;
                        } else if (c == ClassLoader.class) {
                            value = this.classLoader;
                        } else if (c == CacheManager.class) {
                            value = this.cacheManager;
                        } else if (c == ConfigurationProperties.class) {
                            value = this.properties;
                        } else {
                            throw new IllegalStateException("Unhandled bean injection value");
                        }
                        m.invoke(builder, value);
                    } catch (IllegalAccessException | InvocationTargetException ex) {
                        throw new RecognitionException(Helpers.resolveThrowableException(ex), parser, stream, ctx);
                    }
                });
            }
            try {
                Object created = builder.build();
                stack.push(new ObjectWrapped<>(created));
            } catch (Exception e) {
                throw new RecognitionException(Helpers.resolveThrowableException(e), parser, stream, ctx);
            }
        } else {
            stack.push(wobject);
        }
    }

    @Override
    public void exitPipenode(PipenodeContext ctx) {
        Object o = stack.pop();
        if( (o instanceof ObjectWrapped) ) {
            @SuppressWarnings("unchecked")
            ObjectWrapped<Processor> processor = (ObjectWrapped<Processor>) o;
            ProcessorInstance pi = new ProcessorInstance(processor);
            stack.push(pi);
        } else if( (o instanceof PipenodesList) ) {
            PipenodesList pipes = (PipenodesList) o;
            Pipeline p = parsePipeline(pipes, currentPipeLineName);
            AnonymousSubPipeline anonymous = new AnonymousSubPipeline();
            anonymous.setPipeline(p);
            stack.push(new ProcessorInstance(anonymous));
        } else if( (o instanceof PipeRef) ) {
            PipeRef pr = (PipeRef) o;
            NamedSubPipeline nsp = new NamedSubPipeline();
            nsp.setPipeRef(pr.pipename);
            stack.push(new ProcessorInstance(nsp));
        } else {
            assert false : "unwanted object in stack: " + o.getClass();
        }
    }

    @Override
    public void exitForkpiperef(ForkpiperefContext ctx) {
        Forker fk = new Forker();
        fk.setDestination(ctx.identifier().getText());
        ProcessorInstance pi = new ProcessorInstance(fk);
        stack.push(pi);
    }

    @Override
    public void exitForwardpiperef(ForwardpiperefContext ctx) {
        Forwarder fw = new Forwarder();
        fw.setDestination(ctx.identifier().getText());
        ProcessorInstance pi = new ProcessorInstance(fw);
        stack.push(pi);
    }

    private Processor getProcessor(Pipenode i, String currentPipeLineName) throws ConfigException {
        Processor t;
        if (i instanceof ConfigListener.PipeRef){
            ConfigListener.PipeRef cpr = (ConfigListener.PipeRef) i;
            NamedSubPipeline pr = new NamedSubPipeline();
            pr.setPipeRef(cpr.pipename);
            t = pr;
        } else if (i instanceof ConfigListener.PipenodesList){
            AnonymousSubPipeline subpipe = new AnonymousSubPipeline();
            Pipeline p = parsePipeline((ConfigListener.PipenodesList)i, currentPipeLineName);
            subpipe.setPipeline(p);
            t = subpipe;
        } else if (i instanceof ConfigListener.ProcessorInstance) {
            ConfigListener.ProcessorInstance pi = (ConfigListener.ProcessorInstance) i;
            t = pi.wrapped;
        } else {
            throw new IllegalStateException("Unreachable code for " + i);
        }
        return t;
    }

    Pipeline parsePipeline(PipenodesList desc, String currentPipeLineName) throws ConfigException {
        List<Processor> allSteps = new ArrayList<>() {
            @Override
            public String toString() {
                StringBuilder buffer = new StringBuilder();
                buffer.append("PipeList(");
                for (Processor i : this) {
                    buffer.append(i);
                    buffer.append(", ");
                }
                buffer.setLength(buffer.length() - 2);
                buffer.append(')');
                return buffer.toString();
            }
        };

        desc.processors.stream().map(i -> getProcessor(i, currentPipeLineName)).forEach(allSteps::add);
        return new Pipeline(allSteps, depth == 0 ? currentPipeLineName : null, desc.nextPipelineName);
    }

    int depth = 0;
    @Override
    public void enterPipeline(PipelineContext ctx) {
        depth++;
        currentPipeLineName = ctx.identifier().getText();
    }

    @Override
    public void exitPipeline(PipelineContext ctx) {
        FinalpiperefContext nextpipe = ctx.finalpiperef();
        if(nextpipe != null) {
            // The PipeRefName was useless
            stack.pop();
        }
        PipenodesList pipe;
        if( ! stack.isEmpty()) {
            pipe = (PipenodesList) stack.pop();
        } else {
            // Empty pipeline, was not created in exitPipenodeList
            pipe = new PipenodesList();
        }
        if(nextpipe != null) {
            pipe.nextPipelineName = nextpipe.getText();
        }
        pipelines.put(currentPipeLineName, pipe);
        logger.debug("Adding new pipeline {}", currentPipeLineName);
        currentPipeLineName = null;
        depth--;
    }

    @Override
    public void enterPipenodeList(PipenodeListContext ctx) {
        stack.push(StackMarker.PIPE_NODE_LIST);
    }

    @Override
    public void exitPipenodeList(PipenodeListContext ctx) {
        PipenodesList pipe = new PipenodesList();
        Object o;
        do {
            o = stack.pop();
            if (o instanceof ProcessorInstance) {
                pipe.processors.add(0, (ProcessorInstance)o);
            }
            assert StackMarker.PIPE_NODE_LIST == o || (o instanceof ProcessorInstance) : o.getClass();
        } while(StackMarker.PIPE_NODE_LIST != o);
        stack.push(pipe);
    }

    @Override
    public void enterPath(PathContext ctx) {
        stack.push(StackMarker.PATH);
        WrapEvent we = new WrapEvent();
        we.setPathArray(convertEventVariable(ctx.eventVariable()));
        ProcessorInstance pi = new ProcessorInstance(we);
        stack.push(pi);
    }

    @Override
    public void exitPath(PathContext ctx) {
        UnwrapEvent we = new UnwrapEvent();
        ProcessorInstance pi = new ProcessorInstance(we);
        stack.push(pi);
        PipenodesList pipe = new PipenodesList();
        Object o;
        do {
            o = stack.pop();
            if (o instanceof ProcessorInstance) {
                pipe.processors.add(0, (ProcessorInstance)o);
            }
            assert StackMarker.PATH == o || (o instanceof ProcessorInstance) : o.getClass();
        } while(StackMarker.PATH != o);
        stack.push(pipe);
    }

    @Override
    public void exitPiperef(PiperefContext ctx) {
        // In pipenode, part of a pipeline, expect to find a transformer, so transform the name to a PipeRef transformer
        // Other case the name is kept as is
        if(ctx.getParent() instanceof loghub.RouteParser.PipenodeContext) {
            PipeRef piperef = new PipeRef();
            piperef.pipename = ((PipeRefName) stack.pop()).piperef;
            stack.push(piperef);
        }
    }

    @Override
    public void enterTestExpression(TestExpressionContext ctx) {
        stack.push(StackMarker.TEST);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void exitTest(TestContext ctx) {
        Test test = new Test();
        List<Pipenode> clauses = new ArrayList<>(2);
        Object o;
        do {
            o = stack.pop();
            if (o instanceof Pipenode) {
                Pipenode t = (Pipenode) o;
                clauses.add(0, t);
            } else if (o instanceof ObjectWrapped) {
                test.setTest(resolveWrappedExpression((ObjectWrapped<?>)o));
            }
        } while (StackMarker.TEST != o);
        assert clauses.size() == 1 || clauses.size() == 2;
        Processor thenProcessor = getProcessor(clauses.get(0), currentPipeLineName);
        test.setThen(thenProcessor);
        if (clauses.size() == 2) {
            Processor elseProcessor = getProcessor(clauses.get(1), currentPipeLineName);
            test.setElse(elseProcessor);
        }
        stack.push(new ProcessorInstance(test));
    }

    @Override
    public void enterInputObjectlist(InputObjectlistContext ctx) {
        stack.push(StackMarker.OBJECT_LIST);
    }

    @Override
    public void exitInputObjectlist(InputObjectlistContext ctx) {
        List<ObjectWrapped<?>> l = new ArrayList<>();
        while(StackMarker.OBJECT_LIST != stack.peek()) {
            l.add((ObjectWrapped<?>) stack.pop());
        }
        stack.pop();
        stack.push(l);
    }

    @Override
    public void enterOutputObjectlist(OutputObjectlistContext ctx) {
        stack.push(StackMarker.OBJECT_LIST);
    }

    @Override
    public void exitOutputObjectlist(OutputObjectlistContext ctx) {
        List<ObjectWrapped<?>> l = new ArrayList<>();
        while(StackMarker.OBJECT_LIST != stack.peek()) {
            l.add((ObjectWrapped<?>) stack.pop());
        }
        stack.pop();
        stack.push(l);
    }

    @Override
    public void exitOutput(OutputContext ctx) {
        PipeRefName piperef;
        @SuppressWarnings("unchecked")
        List<ObjectWrapped<Sender>> senders = (List<ObjectWrapped<Sender>>) stack.pop();
        if (stack.peek() instanceof PipeRefName) {
            piperef = (PipeRefName) stack.pop();
        } else {
            // if no pipe name given, take events from the main pipe
            piperef = new PipeRefName("main");
        }
        if (outputPipelines.contains(piperef.piperef)) {
            throw new RecognitionException("already sent pipeline " + piperef.piperef, parser, stream, ctx);
        }
        outputPipelines.add(piperef.piperef);
        Output output = new Output(senders, piperef.piperef);
        outputs.add(output);
        logger.debug("adding new output {}", output);
    }

    @Override
    public void exitInput(InputContext ctx) {
        PipeRefName piperef;
        if(stack.peek() instanceof PipeRefName) {
            piperef = (PipeRefName) stack.pop();
        } else {
            // if no pipe name given, events are sent to the main pipe
            piperef = new PipeRefName("main");
        }
        List<ObjectWrapped<Receiver>> receivers = stack.popTyped();
        Input input = new Input(receivers, piperef.piperef);
        inputs.add(input);
        logger.debug("adding new input {}", input);
    }

    @Override
    public void exitProperty(PropertyContext ctx) {
        assert stack.peek() instanceof ObjectWrapped;
        stack.pop();
    }

    @Override
    public void exitSourcedef(SourcedefContext ctx) {
        @SuppressWarnings("unchecked")
        ObjectWrapped<Source> source = (ObjectWrapped<Source>) stack.pop();
        sources.get(ctx.identifier().getText()).set(source.wrapped);
    }

    @Override
    public void enterArray(ArrayContext ctx) {
        stack.push(StackMarker.ARRAY);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void exitArray(ArrayContext ctx) {
        List<Object> array = new ArrayList<>();
        while (StackMarker.ARRAY != stack.peek()) {
            Object o = stack.pop();
            if (o instanceof ObjectWrapped) {
                o = ((ObjectWrapped<Object>) o).wrapped;
            }
            array.add(o);
        }
        Collections.reverse(array);
        stack.pop();
        stack.push(new ObjectWrapped<>(array.toArray()));
    }

    @Override
    public void exitDrop(DropContext ctx) {
        ObjectWrapped<Drop> drop = new ObjectWrapped<>(new Drop());
        stack.push(drop);
    }

    @Override
    public void enterFire(FireContext ctx) {
        stack.push(StackMarker.FIRE);
    }

    @Override
    public void exitFire(FireContext ctx) {
        FireEvent fire = new FireEvent();
        Map<VariablePath, Expression> fields = new HashMap<>();
        int count = ctx.eventVariable().size() - 1;
        while (StackMarker.FIRE != stack.peek()) {
            Object o = stack.pop();
            if (o instanceof ObjectWrapped) {
                VariablePath lvalue = convertEventVariable(ctx.eventVariable().get(count--));
                Expression expression = resolveWrappedExpression((ObjectWrapped<?>) o);
                fields.put(lvalue, expression);
            } else if (o instanceof PipeRefName){
                PipeRefName name = (PipeRefName) o;
                fire.setDestination(name.piperef);
            } else {
                throw new RecognitionException("invalid fire argument: " + o, parser, stream, ctx);
            }
        }
        fire.setFields(fields);
        stack.pop();
        stack.push(new ObjectWrapped<>(fire));
    }

    @Override
    public void exitLog(LogContext ctx) {
        Expression expression = resolveWrappedExpression();
        Log.Builder builder = Log.getBuilder();
        builder.setMessage(expression);
        builder.setLevel(ctx.level().getText());
        Log log = builder.build();
        stack.push(new ObjectWrapped<>(log));
    }

    @Override
    public void enterEtl(EtlContext ctx) {
        stack.push(StackMarker.ETL);
    }

    private VariablePath convertEventVariable(EventVariableContext ev) {
        if (ev.ts != null) {
            return VariablePath.TIMESTAMP;
        } else if (ev.lex != null) {
            return VariablePath.LASTEXCEPTION;
        } else if (ev.ctx != null && ev.vp1 == null ) {
            return VariablePath.ofContext(Collections.emptyList());
        } else if (ev.ctx != null) {
            return VariablePath.ofContext(convertEventVariable(ev.vp1));
        } else if (ev.MetaName() != null) {
            return VariablePath.ofMeta(ev.MetaName().getText().substring(1));
        } else if (ev.vp2 != null) {
            List<String> path = convertEventVariable(ev.vp2);
            if (ev.root != null) {
                path.add(0, ".");
            }
            if (ev.indirect != null) {
                return VariablePath.ofIndirect(path);
            } else {
                return VariablePath.of(path);
            }
        } else {
            return VariablePath.parse(".");
        }
    }
    private List<String> convertEventVariable(VarPathContext vp) {
        if (vp.QualifiedIdentifier() != null) {
            return VariablePath.pathElements(vp.QualifiedIdentifier().getText());
        } else {
            return vp.pathElement().stream().map(this::filterpathElement).collect(Collectors.toList());
        }
    }

    private String filterpathElement(PathElementContext pec) {
        return pec.children.get(0).getText();
    }

    @Override
    public void exitEtl(EtlContext ctx) {
        // Check that the lvalue (the destination) is not the context, it's read only
        if (ctx.eventVariable(0) != null && ctx.eventVariable(0).ctx != null) {
            throw new RecognitionException("Context can't be a lvalue for " + ctx.getText(), parser, stream, ctx);
        }

        Etl etl;

        switch(ctx.op.getText()) {
        case("-"):
            etl = new Etl.Remove();
        break;
        case("<"): {
            etl = new Etl.Rename();
            ((Etl.Rename) etl).setSource(convertEventVariable(ctx.eventVariable().get(1)));
            break;
        }
        case("="): {
            Expression expression = resolveWrappedExpression();
            etl = new Etl.Assign();
            ((Etl.Assign)etl).setExpression(expression);
            break;
        }
        case("=+"): {
            Expression expression = resolveWrappedExpression();
            etl = new Etl.Append();
            ((Etl.Append)etl).setExpression(expression);
            break;
        }
        case("("): {
            etl = new Etl.Convert();
            ((Etl.Convert)etl).setClassName(ctx.QualifiedIdentifier().getText());
            break;
        }
        case("@"): {
            etl = new Mapper();
            ObjectWrapped<Map<Object, Object>> wrapmap = stack.popTyped();
            Map<Object, Object> map = wrapmap.wrapped;
            Expression expression = resolveWrappedExpression();
            ((Mapper) etl).setMap(map);
            ((Mapper) etl).setExpression(expression);
            break;
        }
        default:
            throw new RecognitionException("invalid operator " + ctx.op.getText(), parser, stream, ctx);
        }
        // Remove Etl marker
        Object o = stack.pop();
        assert StackMarker.ETL == o;
        etl.setLvalue(convertEventVariable(ctx.eventVariable().get(0)));
        stack.push(new ProcessorInstance(etl));
    }

    @Override
    public void enterMap(MapContext ctx) {
        stack.push(StackMarker.MAP);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void exitMap(MapContext ctx) {
        if (ctx.source() == null) {
            Map<Object, Object> map = new HashMap<>();
            Object o;
            while((o = stack.pop()) != StackMarker.MAP) {
                Object value;
                if (o instanceof ObjectWrapped) {
                    value = ((ObjectWrapped<Object>) o).wrapped;
                } else {
                    value = o.getClass();
                }
                ObjectWrapped<Object> key = (ObjectWrapped<Object>) stack.pop();
                map.put(key.wrapped, value);
            }
            stack.push(new ObjectWrapped<Map<?, ?>>(map));
        } else {
            Object o = stack.pop();
            assert o == ConfigListener.StackMarker.MAP;
            // Don't forget to remove the initial %
            String sourceName = ctx.source().getText().substring(1);
            if (! sources.containsKey(sourceName)) {
                throw new RecognitionException("Undefined source " + sourceName, parser, stream, ctx);
            }
            Source s = sources.get(sourceName).get();
            stack.push(new ObjectWrapped<>(s));
        }
    }

    @Override
    public void enterExpressionsList(ExpressionsListContext ctx) {
        stack.push(StackMarker.EXPRESSION_LIST);
    }

    @Override
    public void exitExpressionsList(ExpressionsListContext ctx) {
        List<ExpressionBuilder> expressionsList = new ArrayList<>();
        Object se;
        while ((se = stack.pop()) != StackMarker.EXPRESSION_LIST) {
            expressionsList.add((ExpressionBuilder)se);
        }
        Collections.reverse(expressionsList);
        stack.push(expressionsList);
    }

    @Override
    public void enterExpression(ExpressionContext ctx) {
        expressionDepth++;
    }

    private static final Pattern regexContent = Pattern.compile("\\\\([\r\n]){1,2}?");

    @Override
    public void exitPattern(RouteParser.PatternContext ctx) {
        try {
            String patternDef = ctx.Pattern().getText();
            int wrapperCount = patternDef.charAt(0) == '/' ? 1 : 3;
            // Remove the wrapping / .. /
            patternDef = patternDef.substring(wrapperCount, patternDef.length() - wrapperCount);
            patternDef = regexContent.matcher(patternDef).replaceAll("$1");
            Pattern pattern = patternCache.computeIfAbsent(patternDef, Pattern::compile);
            stack.push(new ObjectWrapped<>(pattern));
        } catch (PatternSyntaxException e) {
            throw new RecognitionException(Helpers.resolveThrowableException(e), parser, stream, ctx);
        }
    }

    @Override
    public void exitExpression(ExpressionContext ctx) {
        ExpressionBuilder expression;
        if (ctx.sl != null) {
            List<ExpressionBuilder> exlist = ctx.expressionsList() != null ? stack.popTyped() : null;
            ObjectWrapped<String> stringWrapped = stack.popTyped();
            String format = stringWrapped.wrapped;
            VarFormatter vf = new VarFormatter(format);
            if (vf.isEmpty()) {
                expression = ExpressionBuilder.of(format);
            } else  if (exlist != null) {
                ExpressionBuilder expressions = ExpressionBuilder.of(exlist);
                Expression.ExpressionLambda listLambda = expressions.getPayload();
                expression = ExpressionBuilder.of(ed -> vf.format(listLambda.apply(ed)));
            } else {
                expression = ExpressionBuilder.of(vf);
            }
        } else if (ctx.nl != null) {
            stack.pop();
            expression = ExpressionBuilder.of(NullOrMissingValue.NULL);
        } else if (ctx.c != null) {
            ObjectWrapped<Character> payload = stack.popTyped();
            expression = ExpressionBuilder.of(payload.wrapped);
        } else if (ctx.l != null) {
            ObjectWrapped<Object> literal = stack.popTyped();
            expression = ExpressionBuilder.of(literal.wrapped);
        } else if (ctx.ev != null) {
            VariablePath path = convertEventVariable(ctx.ev);
            expression = ExpressionBuilder.of(path);
        } else if (ctx.opm != null) {
            ObjectWrapped<Pattern> patternWrap = stack.popTyped();
            Pattern pattern = patternWrap.wrapped;
            ExpressionBuilder pre = stack.popTyped();
            String patterOperator = ctx.opm.getText();
            expression = ExpressionBuilder.of(pre, o -> Expression.regex(o, patterOperator, pattern));
        } else if (ctx.opnotlogical != null) {
            // '!'
            ExpressionBuilder post = stack.popTyped();
            expression = ExpressionBuilder.of(post, o -> ! Expression.asBoolean(o));
        } else if (ctx.opnotbinary != null) {
            // '.~'
            ExpressionBuilder post = stack.popTyped();
            expression = ExpressionBuilder.of(post, o -> Expression.groovyOperator("~", o));
        } else if (ctx.op3 != null) {
            // '+'|'-'
            ExpressionBuilder post = stack.popTyped();
            String op3 = ctx.op3.getText();
            ExpressionBuilder sign = ExpressionBuilder.of("-".equals(op3) ? -1 : 1);
            expression = ExpressionBuilder.of(sign, "*", post);
        } else if (ctx.opinfix != null) {
            // '*'|'/'|'%' |'+'|'-'|'<<'|'>>'|'>>>'|'**'
            String op = ctx.opinfix.getText();
            ExpressionBuilder post = stack.popTyped();
            ExpressionBuilder pre = stack.popTyped();
            expression = ExpressionBuilder.of(pre, op, post);
        } else if (ctx.opin != null) {
            // 'in'|'!in'
            String op = ctx.opin.getText();
            ExpressionBuilder post = stack.popTyped();
            ExpressionBuilder pre = stack.popTyped();
            expression = ExpressionBuilder.of(pre, post, (o1, o2) -> Expression.in(op, o1, o2));
        } else if (ctx.opinstance != null) {
            try {
                String className = ctx.qualifiedIdentifier().getText();
                Class<?> clazz = classLoader.loadClass(className);
                // 'instanceof'|'!instanceof'
                boolean negated = ctx.neg != null || ctx.opinstance.getText().startsWith("!");
                ExpressionBuilder pre = (ExpressionBuilder) stack.pop();
                expression = ExpressionBuilder.of(pre, o -> Expression.instanceOf(negated, o, clazz));
            } catch (ClassNotFoundException e) {
                throw new RecognitionException(Helpers.resolveThrowableException(e), parser, stream, ctx);
            }
        } else if (ctx.opcomp != null) {
            // '=='|'!='|'<=>' | '==='|'!=='|'<'|'<='|'>'|'>='
            String op = ctx.opcomp.getText();
            ExpressionBuilder post = stack.popTyped();
            ExpressionBuilder pre = stack.popTyped();
            expression = ExpressionBuilder.of(pre, post, (o1, o2) -> Expression.compare(op, o1, o2));
        } else if (ctx.exists != null) {
            // (== | !=) *
            VariablePath path = convertEventVariable(ctx.exists);
            String op = ctx.op.getText();
            expression = ExpressionBuilder.of(ed -> Expression.compare(op, ed.getEvent().getAtPath(path), Expression.ANYVALUE));
        } else if (ctx.opbininfix != null) {
            // '.&'|'.^'|'.|'
            String op = ctx.opbininfix.getText().substring(1);
            ExpressionBuilder post = stack.popTyped();
            ExpressionBuilder pre = stack.popTyped();
            expression = ExpressionBuilder.of(pre, op, post);
        } else if (ctx.op12 != null) {
            // '&&'
            ExpressionBuilder post = stack.popTyped();
            ExpressionBuilder pre = stack.popTyped();
            expression = ExpressionBuilder.of(pre, post, (o1, o2) -> Expression.asBoolean(o1) && Expression.asBoolean(o2));
        } else if (ctx.op13 != null) {
            // '||'
            ExpressionBuilder post = stack.popTyped();
            ExpressionBuilder pre = stack.popTyped();
            expression = ExpressionBuilder.of(pre, post, (o1, o2) -> Expression.asBoolean(o1) || Expression.asBoolean(o2));
        } else if (ctx.e3 != null) {
            expression = stack.popTyped();
        } else if (ctx.newclass != null) {
            Expression.ExpressionLambda argsLambda;
            if (ctx.expressionsList() != null) {
                List<ExpressionBuilder> exlist = stack.popTyped();
                ExpressionBuilder expressions = ExpressionBuilder.of(exlist);
                argsLambda = expressions.getPayload();
            } else {
                argsLambda = ed -> List.of();
            }
            try {
                Class<?> theClass = classLoader.loadClass(ctx.newclass.getText());
                expression = ExpressionBuilder.of(ed -> Expression.newInstance(theClass,
                        (List<Object>) argsLambda.apply(ed)));
            } catch (ClassNotFoundException e) {
                throw new RecognitionException("Unknown class: " + ctx.newclass.getText(), parser, stream, ctx);
            }
        } else if (ctx.arrayIndex != null) {
            int arrayIndexSign = ctx.arrayIndexSign != null ? -1 : 1;
            int arrayIndex = ((ObjectWrapped<Number>) stack.popTyped()).wrapped.intValue() * arrayIndexSign;
            ExpressionBuilder subexpression = stack.popTyped();
            expression = ExpressionBuilder.of(subexpression, o -> Expression.getIterableIndex(o, arrayIndex));
        } else if (ctx.stringFunction != null) {
            ExpressionBuilder subexpression = stack.popTyped();
            String stringFunction = ctx.stringFunction.getText();
            expression = ExpressionBuilder.of(subexpression, o -> Expression.stringFunction(stringFunction, o));
        } else if (ctx.join != null) {
            ExpressionBuilder source = stack.popTyped();
            ObjectWrapped<String> joinWrapper = stack.popTyped();
            expression = ExpressionBuilder.of(source, o -> Expression.join(joinWrapper.wrapped, o));
        } else if (ctx.split != null) {
            ExpressionBuilder source = stack.popTyped();
            Pattern pattern;
            if (ctx.pattern() != null) {
                ObjectWrapped<Pattern> patternWrapper = stack.popTyped();
                pattern = patternWrapper.wrapped;
            } else {
                ObjectWrapped<String> patternWrapper = stack.popTyped();
                try {
                    pattern = Pattern.compile(patternWrapper.wrapped);
                } catch (PatternSyntaxException ex) {
                    throw new RecognitionException(Helpers.resolveThrowableException(ex), parser, stream, ctx);
                }
            }
            expression = ExpressionBuilder.of(source, o -> Expression.split(o, pattern));
        } else if (ctx.gsub != null) {
            ObjectWrapped<String> substitution = stack.popTyped();
            ObjectWrapped<Pattern> patternWrapped  = stack.popTyped();
            ExpressionBuilder source = stack.popTyped();
            expression = ExpressionBuilder.of(source,
                    o -> Expression.gsub(o, patternWrapped.wrapped, substitution.wrapped));
        } else if (ctx.now != null) {
            expression = ExpressionBuilder.of(ed -> Instant.now());
        } else if (ctx.isEmpty != null) {
            ExpressionBuilder subexpression = stack.popTyped();
            expression = ExpressionBuilder.of(subexpression, Expression::isEmpty);
        } else if (ctx.isIp != null) {
            ExpressionBuilder subexpression = stack.popTyped();
            expression = ExpressionBuilder.of(subexpression, Expression::isIpAddress);
        } else if (ctx.collection != null) {
            String collectionType = ctx.collection.getText();
            if (ctx.expressionsList() == null) {
                expression = ExpressionBuilder.of(ed -> Expression.newCollection(collectionType));
            } else {
                List<ExpressionBuilder> exlist = stack.popTyped();
                ExpressionBuilder expressions = ExpressionBuilder.of(exlist);
                expression = ExpressionBuilder.of(expressions, o -> Expression.asCollection(collectionType, o));
            }
        } else if (ctx.lambdavar != null) {
            expression = ExpressionBuilder.of(Expression.ExpressionData::getValue);
        } else {
            throw new IllegalStateException("Unreachable code");
        }
        expressionDepth--;
        if (expressionDepth == 0) {
            String expressionSource;
            if (stream instanceof CharStream) {
                CharStream cs = (CharStream) stream;
                Interval i = Interval.of(ctx.start.getStartIndex(), ctx.stop.getStopIndex());
                expressionSource = cs.getText(i);
            } else {
                expressionSource = ctx.getText();
            }
            Expression expr = expression.build(expressionSource);
            if (expression.getType() == ExpressionBuilder.ExpressionType.LITERAL) {
                try {
                    stack.push(new ObjectWrapped<>(expr.eval()));
                } catch (ProcessorException e) {
                    throw new RecognitionException("Invalid expression \"" + expressionSource + "\": " + Helpers.resolveThrowableException(e), parser, stream, ctx);
                }
            } else {
                stack.push(new ObjectWrapped<>(expr));
            }
        } else {
            stack.push(expression);
        }
    }

    @Override
    public void exitLambda(RouteParser.LambdaContext ctx) {
        ObjectWrapped<Expression> exw = stack.popTyped();
        stack.push(new ObjectWrapped<>(new Lambda(exw.wrapped)));
    }

    @SuppressWarnings("unchecked")
    private  Expression resolveWrappedExpression() {
        return resolveWrappedExpression((ObjectWrapped<Expression>) stack.pop());
    }

    private  Expression resolveWrappedExpression(ObjectWrapped<?> wrapper) {
        if (wrapper.wrapped instanceof Expression) {
            return (Expression) wrapper.wrapped;
        } else {
            return new Expression(wrapper.wrapped);
        }
    }

    static Number resolveNumberLiteral(IntegerLiteralContext ctx) {
        int base;
        String text;
        if (ctx.BinaryDigits() != null) {
            base = 2;
            text = ctx.BinaryDigits().getText().substring(2);
        } else if (ctx.HexDigits() != null) {
            base = 16;
            text = ctx.HexDigits().getText().substring(2);
        } else if (ctx.OctalDigits() != null) {
            base = 8;
            text = ctx.OctalDigits().getText().substring(2);
        } else {
            base = 10;
            text = ctx.DecimalDigits().getText();
        }
        Number litteral;
        try {
            litteral = Integer.valueOf(text, base);
        } catch (NumberFormatException nfe) {
            try {
                litteral = Long.valueOf(text, base);
            } catch (NumberFormatException e) {
                litteral = new BigInteger(text, base);
            }
        }
        return litteral;
    }

}
