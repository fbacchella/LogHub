package loghub.configuration;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.IntStream;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.pattern.TokenTagToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.Event;
import loghub.Helpers;
import loghub.Pipeline;
import loghub.Processor;
import loghub.Receiver;
import loghub.RouteBaseListener;
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
import loghub.RouteParser.PipelineContext;
import loghub.RouteParser.PipenodeContext;
import loghub.RouteParser.PipenodeListContext;
import loghub.RouteParser.PiperefContext;
import loghub.RouteParser.PropertyContext;
import loghub.RouteParser.SourcedefContext;
import loghub.RouteParser.StringLiteralContext;
import loghub.RouteParser.TestContext;
import loghub.RouteParser.TestExpressionContext;
import loghub.Sender;
import loghub.Source;
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

class ConfigListener extends RouteBaseListener {

    private static final Logger logger = LogManager.getLogger();

    private static enum StackMarker {
        Test,
        ObjectList,
        PipeNodeList,
        Array,
        Expression,
        ExpressionList,
        Fire,
        Etl,
        Map;
    };

    static final class Input {
        final List<ObjectWrapped<Receiver>> receiver;
        String piperef;
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

    static interface Pipenode {};

    static final class PipenodesList implements Pipenode {
        final List<ProcessorInstance> processors = new ArrayList<>();
        String nextPipelineName;
    }

    static final class PipeRef implements Pipenode {
        String pipename;
    }

    static final class PipeRefName implements Pipenode {
        final String piperef;
        private PipeRefName(String piperef) {
            this.piperef = piperef;
        }
    }

    static interface ObjectReference {};

    static class  ObjectWrapped<T> implements ObjectReference {
        final T wrapped;
        ObjectWrapped(T wrapped) {
            this.wrapped = wrapped;
        }
    }

    public static class SourceProvider {
        public Source source; 
        public Map<Object, Object> map;
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

    };

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
    };

    final TypedStack stack = new TypedStack();

    final Map<String, PipenodesList> pipelines = new HashMap<>();
    final List<Input> inputs = new ArrayList<>();
    final List<Output> outputs = new ArrayList<>();
    final Map<String, Object> properties = new HashMap<>();
    final Map<String, String> formatters = new HashMap<>();
    final Map<String, SourceProvider> sources = new HashMap<>();
    final Set<String> outputPipelines = new HashSet<>();

    private String currentPipeLineName = null;
    private int expressionDepth = 0;

    private Set<String> lockedProperties = new HashSet<>();

    Parser parser;
    IntStream stream;
    ClassLoader classLoader = ConfigListener.class.getClassLoader();

    @Override
    public void enterPiperef(PiperefContext ctx) {
        stack.push(new PipeRefName(ctx.getText()));
    }

    private void pushLiteral(ParserRuleContext ctx, Object content) {
        // Don't keep literal in a expression, they will be managed in groovy
        if(expressionDepth > 0) {
            return;
        } else {
            stack.push(new ObjectWrapped<Object>(content));
        }
    }

    @Override
    public void enterFloatingPointLiteral(FloatingPointLiteralContext ctx) {
        String content = ctx.FloatingPointLiteral().getText();
        pushLiteral(ctx, Double.valueOf(content));
    }

    @Override
    public void enterCharacterLiteral(CharacterLiteralContext ctx) {
        String content = ctx.CharacterLiteral().getText();
        pushLiteral(ctx, content.charAt(0));
    }

    @Override
    public void enterStringLiteral(StringLiteralContext ctx) {
        String content = ctx.StringLiteral().getText();
        pushLiteral(ctx, content);
    }

    @Override
    public void enterIntegerLiteral(IntegerLiteralContext ctx) {
        String content = ctx.IntegerLiteral().getText();
        pushLiteral(ctx, Integer.valueOf(content));
    }

    @Override
    public void enterBooleanLiteral(BooleanLiteralContext ctx) {
        String content = ctx.getText();
        pushLiteral(ctx, Boolean.valueOf(content));
    }

    @Override
    public void enterNullLiteral(NullLiteralContext ctx) {
        pushLiteral(ctx, null);
    }

    @Override
    public void exitBean(BeanContext ctx) {
        String beanName = null;
        ObjectWrapped<Object> beanValue = null;
        if(ctx.condition != null) {
            beanName = ctx.condition.getText();
            beanValue = stack.popTyped();
        } else if (ctx.expression() != null) {
            beanName = "if";
            beanValue = stack.popTyped();
        } else {
            beanName = ctx.beanName().getText();
            beanValue = stack.popTyped();
        }
        ObjectWrapped<Object> beanObject = stack.peekTyped();
        assert (beanName != null);
        assert (beanValue != null);
        try {
            BeansManager.beanSetter(beanObject.wrapped, beanName, beanValue.wrapped);
        } catch (InvocationTargetException e) {
            throw new RecognitionException(Helpers.resolveThrowableException(e.getTargetException()), parser, stream, ctx);
        }
    }

    @Override
    public void exitMergeArgument(MergeArgumentContext ctx) {
        String beanName = ctx.type.getText();
        ObjectWrapped<Object> beanValue;
        beanValue = stack.popTyped();
        ObjectWrapped<Object> beanObject = stack.peekTyped();
        assert (beanName != null);
        assert (beanValue != null);

        try {
            BeansManager.beanSetter(beanObject.wrapped, beanName, beanValue.wrapped);
        } catch (InvocationTargetException e) {
            throw new RecognitionException(Helpers.resolveThrowableException(e), parser, stream, ctx);
        }
    }

    @Override
    public void enterMerge(MergeContext ctx) {
        stack.push(new ObjectWrapped<Merge>(new Merge()));
    }

    ObjectWrapped<Object> getObject(String qualifiedName, ParserRuleContext ctx) {
        try {
            logger.debug("Load ing {} with {}", qualifiedName, classLoader);
            Class<?> objectClass = classLoader.loadClass(qualifiedName);
            return new ObjectWrapped<Object>(objectClass.newInstance());
        } catch (ClassNotFoundException e) {
            throw new RecognitionException("Unknown class " + qualifiedName, parser, stream, ctx);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RecognitionException("Unsuable class " + qualifiedName, parser, stream, ctx);
        }
    }

    @Override
    public void enterObject(ObjectContext ctx) {
        if (stack.peek() instanceof SourceProvider) {
            SourceProvider sp = stack.popTyped();
            ObjectWrapped<Source> wrapper = new ObjectWrapped<>(sp.source);
            stack.push(wrapper);
        } else {
            String qualifiedName = ctx.QualifiedIdentifier().getText();
            stack.push(getObject(qualifiedName, ctx));
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
        fk.setDestination(ctx.Identifier().getText());
        ProcessorInstance pi = new ProcessorInstance(fk);
        stack.push(pi);
    }

    @Override
    public void exitForwardpiperef(ForwardpiperefContext ctx) {
        Forwarder fw = new Forwarder();
        fw.setDestination(ctx.Identifier().getText());
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
            throw new RuntimeException("Unreachable code for " + i);
        }
        return t;
    }

    Pipeline parsePipeline(PipenodesList desc, String currentPipeLineName) throws ConfigException {
        List<Processor> allSteps = new ArrayList<Processor>() {
            @Override
            public String toString() {
                StringBuilder buffer = new StringBuilder();
                buffer.append("PipeList(");
                for(Processor i: this) {
                    buffer.append(i);
                    buffer.append(", ");
                }
                buffer.setLength(buffer.length() - 2);
                buffer.append(')');
                return buffer.toString();
            }
        };

        desc.processors.stream().map(i -> {
            return getProcessor(i, currentPipeLineName);
        }).forEach(allSteps::add);
        Pipeline pipe = new Pipeline(allSteps, depth == 0 ? currentPipeLineName : null, desc.nextPipelineName);
        return pipe;
    }

    int depth = 0;
    @Override
    public void enterPipeline(PipelineContext ctx) {
        depth++;
        currentPipeLineName = ctx.Identifier().getText();
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
        stack.push(StackMarker.PipeNodeList );
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
            assert StackMarker.PipeNodeList.equals(o) || (o instanceof ProcessorInstance) : o.getClass();
        } while(! StackMarker.PipeNodeList.equals(o));
        stack.push(pipe);
    }

    @Override
    public void enterPath(PathContext ctx) {
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
        stack.push(StackMarker.Test);
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
                test.setTest(((ObjectWrapped<String>)o).wrapped);
            }
        } while (! StackMarker.Test.equals(o));
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
        stack.push(StackMarker.ObjectList);
    }

    @Override
    public void exitInputObjectlist(InputObjectlistContext ctx) {
        List<ObjectWrapped<?>> l = new ArrayList<>();
        while(! StackMarker.ObjectList.equals(stack.peek())) {
            l.add((ObjectWrapped<?>) stack.pop());
        }
        stack.pop();
        stack.push(l);
    }

    @Override
    public void enterOutputObjectlist(OutputObjectlistContext ctx) {
        stack.push(StackMarker.ObjectList);
    }

    @Override
    public void exitOutputObjectlist(OutputObjectlistContext ctx) {
        List<ObjectWrapped<?>> l = new ArrayList<>();
        while(! StackMarker.ObjectList.equals(stack.peek())) {
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
        if(stack.peek() != null && stack.peek() instanceof PipeRefName) {
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
        @SuppressWarnings("unchecked")
        List<ObjectWrapped<Receiver>> receivers = (List<ObjectWrapped<Receiver>>) stack.pop();
        Input input = new Input(receivers, piperef.piperef);
        inputs.add(input);
        logger.debug("adding new input {}", input);
    }

    @Override
    public void exitProperty(PropertyContext ctx) {
        Object value = stack.pop();
        String key = ctx.propertyName().getText();
        // Avoid reprocess already processed properties
        if (!lockedProperties.contains(key)) {
            properties.put(key, value);
        } else {
            throw new RecognitionException("redefined property", parser, stream, ctx);
        }
    }

    @Override
    public void enterSourcedef(SourcedefContext ctx) {
        String sourceName = ctx.Identifier().getText();
        SourceProvider sp = sources.get(sourceName);
        sp.source.setName(sourceName);
        stack.push(sp);
    }

    @Override
    public void exitSourcedef(SourcedefContext ctx) {
        stack.pop();
    }

    @Override
    public void enterArray(ArrayContext ctx) {
        stack.push(StackMarker.Array);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void exitArray(ArrayContext ctx) {
        List<Object> array = new ArrayList<>();
        while(! StackMarker.Array.equals(stack.peek()) ) {
            Object o = stack.pop();
            if(o instanceof ObjectWrapped) {
                o = ((ObjectWrapped<Object>) o).wrapped;
            }
            array.add(o);
        }
        Collections.reverse(array);
        stack.pop();
        stack.push(new ObjectWrapped<Object[]>(array.toArray()));
    }

    @Override
    public void exitDrop(DropContext ctx) {
        ObjectWrapped<Drop> drop = new ObjectWrapped<Drop>(new Drop());
        stack.push(drop);
    }

    @Override
    public void enterFire(FireContext ctx) {
        stack.push(StackMarker.Fire);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void exitFire(FireContext ctx) {
        FireEvent fire = new FireEvent();
        Map<String[], String> fields = new HashMap<>();
        int count = ctx.eventVariable().size() - 1;
        while(! StackMarker.Fire.equals(stack.peek()) ) {
            Object o = stack.pop();
            if(o instanceof ObjectWrapped) {
                String[] lvalue = convertEventVariable(ctx.eventVariable().get(count--));
                fields.put(lvalue, ((ObjectWrapped<String>) o).wrapped);
            } else if (o instanceof PipeRefName){
                PipeRefName name = (PipeRefName) o;
                fire.setDestination(name.piperef);
            } else {
                throw new RecognitionException("invalid fire argument: " + o.toString(), parser, stream, ctx);
            }
        }
        fire.setFields(fields);
        stack.pop();
        stack.push(new ObjectWrapped<FireEvent>(fire));
    }

    @Override
    public void exitLog(LogContext ctx) {
        ObjectWrapped<String> expression = stack.popTyped();
        Log log = new Log();
        log.setLevel(ctx.level().getText());
        log.setPipeName(currentPipeLineName);
        log.setMessage(expression.wrapped);
        stack.push(new ObjectWrapped<Log>(log));
    }

    @Override
    public void enterEtl(EtlContext ctx) {
        stack.push(StackMarker.Etl);
    }

    private static final CommonToken NONE = new TokenTagToken("", Token.INVALID_TYPE);

    private String[] convertEventVariable(EventVariableContext ev) {
        String keyString = Optional.ofNullable(ev.key).orElse(NONE).getText();
        if (Event.TIMESTAMPKEY.equals(keyString)) {
            return new String[] { ev.key.getText() };
        } else if (ev.MetaName() != null) {
            return new String[] { ev.MetaName().getText() };
        } else {
            List<String> path = ev.Identifier().stream().map(i -> i.getText()).collect(Collectors.toList());
            if (Event.CONTEXTKEY.equals(keyString))
                path.add(0, ev.key.getText());
            path.stream().toArray(String[]::new);
            return path.stream().toArray(String[]::new);
        }
    }

    @Override
    public void exitEtl(EtlContext ctx) {

        // Check that the lvalue (the destination) is not the context, it's read only
        Token root = ctx.eventVariable(0).key;
        if (root != null && Event.CONTEXTKEY.equals(root.getText())) {
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
            @SuppressWarnings("unchecked")
            ObjectWrapped<String> expression = (ObjectWrapped<String>) stack.pop();
            etl = new Etl.Assign();
            ((Etl.Assign)etl).setExpression(expression.wrapped);
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
            ObjectWrapped<String> expression = stack.popTyped();
            ((Mapper) etl).setMap(map);
            ((Mapper) etl).setExpression(expression.wrapped);
            break;
        }
        default:
            throw new RecognitionException("invalid operator " + ctx.op.getText(), parser, stream, ctx);
        }
        // Remove Etl marker
        Object o = stack.pop();
        assert StackMarker.Etl.equals(o);
        etl.setLvalue(convertEventVariable(ctx.eventVariable().get(0)));
        stack.push(new ProcessorInstance(etl));
    }

    @Override
    public void enterMap(MapContext ctx) {
        stack.push(StackMarker.Map);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void exitMap(MapContext ctx) {
        if (ctx.source() == null) {
            Map<Object, Object> map = new HashMap<>();
            Object o;
            while((o = stack.pop()) != StackMarker.Map) {
                Object value;
                if (o instanceof ObjectWrapped) {
                    value = ((ObjectWrapped<Object>) o).wrapped;
                } else {
                    value = o.getClass();
                }
                ObjectWrapped<Object> key = (ObjectWrapped<Object>) stack.pop();
                map.put(key.wrapped, value);
            };
            stack.push(new ObjectWrapped<Map<?, ?>>(map));
        } else {
            Object o = stack.pop();
            assert o == ConfigListener.StackMarker.Map;
            // Don't forget to remove the initial %
            String sourceName = ctx.source().getText().substring(1);
            if (! sources.containsKey(sourceName)) {
                throw new RecognitionException("Undefined source " + sourceName, parser, stream, ctx);
            }
            Source s = sources.get(sourceName).source;
            stack.push(new ObjectWrapped<Source>(s));
        }
    }

    @Override
    public void enterExpressionsList(ExpressionsListContext ctx) {
        stack.push(StackMarker.ExpressionList);
    }

    @Override
    public void exitExpressionsList(ExpressionsListContext ctx) {
        List<String> expressionsList = new ArrayList<>();
        Object se;
        while ((se = stack.pop()) != StackMarker.ExpressionList) {
            expressionsList.add((String)se);
        }
        Collections.reverse(expressionsList);
        stack.push(expressionsList.toString());
    }

    @Override
    public void enterExpression(ExpressionContext ctx) {
        expressionDepth++;
    }

    @Override
    public void exitExpression(ExpressionContext ctx) {
        String expression = null;
        if(ctx.sl != null) {
            String format = ctx.sl.getText();
            String key = "h_" + Integer.toHexString(format.hashCode());
            formatters.put(key, format);
            String subexpression;
            if (ctx.expressionsList() != null) {
                subexpression = (String) stack.pop();
                expression = String.format("formatters.%s.format(%s)", key, subexpression);
            } else {
                expression = String.format("formatters.%s.format(event)", key);
            }
        } else if (ctx.l != null) {
            expression = ctx.l.getText();
        } else if (ctx.ev != null && ctx.ev.MetaName() != null) {
            expression = "event.getMeta(\"" + ctx.ev.MetaName().getText().substring(1) + "\")";
        } else if (ctx.ev != null) {
            StringBuilder buffer = new StringBuilder("event");
            String[] path = convertEventVariable(ctx.ev);
            if (Event.TIMESTAMPKEY.equals(path[0])) {
                buffer.append(".getTimestamp()");
            } else if (Event.CONTEXTKEY.equals(path[0])) {
                buffer.append(".getConnectionContext()");
                Arrays.stream(path, 1, path.length).forEach( i-> {
                    buffer.append(".").append(i);
                });
            } else {
                buffer.append(".getPath(");
                buffer.append(Arrays.stream(path)
                              .map(s -> '"' + s + '"')
                              .collect(Collectors.joining(","))
                                );
                buffer.append(")");
            }
            expression = buffer.toString();
        } else if (ctx.qi != null) {
            expression = ctx.qi.getText();
        } else if (ctx.opu != null) {
            String opu = ctx.opu.getText();
            opu = ".~".equals(opu) ? "~" : opu;
            expression = opu + " " + stack.pop();
        } else if (ctx.opm != null) {
            Object pre = stack.pop();
            expression = pre + " " + ctx.opm.getText() + " " + ctx.patternLiteral().getText();
            if ("=~".equals(ctx.opm.getText())) {
                expression = String.format("(((%s)?:[])[0]?:[])", expression);
            }
        } else if (ctx.opb != null) {
            String opb = ctx.opb.getText();
            // because of use of | as a pipe symbol, it can't be used for the binary 'or'
            // So for users simplicity and consistency, all binary operators are prefixed by a '.'
            // but then it must be removed for groovy
            if(opb.length() == 2 && opb.startsWith(".")) {
                opb = opb.substring(1);
            }
            Object post = stack.pop();
            Object pre = stack.pop();
            expression = pre + " " + opb + " " + post;
        } else if (ctx.e3 != null) {
            Object subexpression = stack.pop();
            expression = "(" + subexpression + ")";
        } else if (ctx.newclass != null) {
            Object subexpression = stack.pop();
            expression = String.format("new %s(%s)", ctx.newclass.getText(), subexpression);
        } else if (ctx.arrayIndex != null) {
            Object subexpression = stack.pop();
            expression = String.format("%s[%s]", subexpression, ctx.arrayIndex.getText());
        }
        expressionDepth--;
        if(expressionDepth == 0) {
            stack.push( new ObjectWrapped<String>(expression));
        } else {
            stack.push(expression);
        }
    }

    /**
     * @param topLevelConfigFile the topLevelConfigFile to set
     */
    public void lockProperty(String property) {
        lockedProperties.add(property);
    }

}
