package loghub.configuration;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import loghub.processors.Drop;
import loghub.processors.Etl;
import loghub.processors.FireEvent;
import loghub.processors.Forker;
import loghub.processors.Forwarder;
import loghub.processors.Log;
import loghub.processors.Mapper;
import loghub.processors.Merge;
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
        final List<ObjectDescription> receiver;
        String piperef;
        Input(List<ObjectDescription>receiver, String piperef) {
            this.piperef = piperef;
            this.receiver = receiver;
        }
        @Override
        public String toString() {
            return "(" + receiver.toString() + " -> " + piperef + ")";
        }
    }

    static final class Output {
        final List<ObjectDescription> sender;
        final String piperef;
        Output(List<ObjectDescription>sender, String piperef) {
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
        final List<Pipenode> processors = new ArrayList<>();
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

    static final class ObjectWrapped implements ObjectReference {
        final Object wrapped;
        ObjectWrapped(Object wrapped) {
            this.wrapped = wrapped;
        }
    }

    static final class Source implements ObjectReference {
        final String source;
        final Map<String, ObjectDescription> sources;
        Source(String source, Map<String, ObjectDescription> sources) {
            this.source = source;
            this.sources = sources;
        }
    }

    static class ObjectDescription implements ObjectReference, Iterable<String> {
        final ParserRuleContext ctx;
        final String clazz;
        final IntStream stream;
        Map<String, ObjectReference> beans = new HashMap<>();
        ObjectDescription(IntStream stream, String clazz, ParserRuleContext ctx) {
            this.clazz = clazz;
            this.ctx = ctx;
            this.stream = stream;
        }
        ObjectReference get(String name) {
            return beans.get(name);
        }
        void put(String name, ObjectReference object) {
            beans.put(name, object);
        }
        @Override
        public Iterator<String> iterator() {
            return beans.keySet().iterator();
        }
    };

    static final class ProcessorInstance extends ObjectDescription implements Pipenode {
        ProcessorInstance(IntStream stream, String clazz, ParserRuleContext ctx) {
            super(stream, clazz, ctx);
        }
        ProcessorInstance(IntStream stream, ObjectDescription object, ParserRuleContext ctx) {
            super(stream, object.clazz, ctx);
            this.beans = object.beans;
        }
    };

    final Deque<Object> stack = new ArrayDeque<>();

    final Map<String, PipenodesList> pipelines = new HashMap<>();
    final List<Input> inputs = new ArrayList<>();
    final List<Output> outputs = new ArrayList<>();
    final Map<String, Object> properties = new HashMap<>();
    final Map<String, String> formatters = new HashMap<>();
    final Map<String, ObjectDescription> sources = new HashMap<>();

    private String currentPipeLineName = null;
    private int expressionDepth = 0;

    private Set<String> lockedProperties = new HashSet<>();

    Parser parser;
    IntStream stream;

    @Override
    public void enterPiperef(PiperefContext ctx) {
        stack.push(new PipeRefName(ctx.getText()));
    }

    private void pushLiteral(ParserRuleContext ctx, Object content) {
        // Don't keep literal in a expression, they will be managed in groovy
        if(expressionDepth > 0) {
            return;
        } else {
            stack.push(new ObjectWrapped(content));
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
        ObjectReference beanValue = null;
        if(ctx.condition != null) {
            beanName = ctx.condition.getText();
            beanValue = new ObjectWrapped(stack.pop());
        } else if (ctx.expression() != null) {
            beanName = "if";
            beanValue = (ObjectReference) stack.pop();
        } else {
            beanName = ctx.beanName().getText();
            beanValue = (ObjectReference) stack.pop();
        }
        ObjectDescription beanObject = (ObjectDescription) stack.peek();
        assert (beanName != null);
        assert (beanValue != null);
        beanObject.put(beanName, beanValue);
    }

    @Override
    public void exitMergeArgument(MergeArgumentContext ctx) {
        String beanName = ctx.type.getText();
        ObjectReference beanValue;
        if ("onFire".equals(beanName) || "onTimeout".equals(beanName)) {
            beanValue = new ObjectWrapped(stack.pop());
        } else {
            beanValue = (ObjectReference) stack.pop();
        }
        ObjectDescription beanObject = (ObjectDescription) stack.peek();
        assert (beanName != null);
        assert (beanValue != null);
        beanObject.put(beanName, beanValue);
    }

    @Override
    public void enterMerge(MergeContext ctx) {
        ObjectReference beanObject = new ObjectDescription(this.stream , Merge.class.getCanonicalName(), ctx);
        stack.push(beanObject);
    }

    @Override
    public void enterObject(ObjectContext ctx) {
        String qualifiedName = ctx.QualifiedIdentifier().getText();
        ObjectReference beanObject = new ObjectDescription(this.stream , qualifiedName, ctx);
        stack.push(beanObject);
    }

    @Override
    public void exitPipenode(PipenodeContext ctx) {
        Object o = stack.pop();
        if( ! (o instanceof Pipenode) ) {
            ObjectDescription object = (ObjectDescription) o;
            ProcessorInstance ti = new ProcessorInstance(this.stream , object, ctx);
            stack.push(ti);
        } else {
            stack.push(o);
        }
    }

    @Override
    public void exitForkpiperef(ForkpiperefContext ctx) {
        ObjectDescription beanObject = new ObjectDescription(this.stream , Forker.class.getCanonicalName(), ctx);
        beanObject.put("destination", new ObjectWrapped(ctx.Identifier().getText()));
        ProcessorInstance ti = new ProcessorInstance(this.stream , beanObject, ctx);
        stack.push(ti);
    }

    @Override
    public void exitForwardpiperef(ForwardpiperefContext ctx) {
        ObjectDescription beanObject = new ObjectDescription(this.stream , Forwarder.class.getCanonicalName(), ctx);
        beanObject.put("destination", new ObjectWrapped(ctx.Identifier().getText()));
        ProcessorInstance ti = new ProcessorInstance(this.stream , beanObject, ctx);
        stack.push(ti);
    }

    @Override
    public void enterPipeline(PipelineContext ctx) {
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
            if (o instanceof Pipenode) {
                pipe.processors.add(0, (Pipenode)o);
            }
            assert (StackMarker.PipeNodeList.equals(o)) || o instanceof Pipenode;
        } while(! StackMarker.PipeNodeList.equals(o));
        stack.push(pipe);
    }

    @Override
    public void enterPath(PathContext ctx) {
        ObjectDescription object = new ObjectDescription(stream, WrapEvent.class.getName(), ctx);
        object.beans.put("pathArray", new ObjectWrapped(convertEventVariable(ctx.eventVariable())));
        ProcessorInstance ti = new ProcessorInstance(stream , object, ctx);
        stack.push(ti);
    }

    @Override
    public void exitPath(PathContext ctx) {
        ObjectDescription object = new ObjectDescription(stream, UnwrapEvent.class.getName(), ctx);
        ProcessorInstance ti = new ProcessorInstance(stream , object, ctx);
        stack.push(ti);
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

    @Override
    public void exitTest(TestContext ctx) {
        ObjectDescription beanObject = new ObjectDescription(this.stream, Test.class.getCanonicalName(), ctx);
        List<Pipenode> clauses = new ArrayList<>(2);
        Object o;
        do {
            o = stack.pop();
            if(o instanceof Pipenode) {
                Pipenode t = (Pipenode) o;
                clauses.add(0, t);
            } else if(o instanceof ObjectWrapped) {
                beanObject.put("test", (ObjectWrapped)o);
            }
        } while(! StackMarker.Test.equals(o));
        beanObject.put("then", new ObjectWrapped(clauses.get(0)));
        if (clauses.size() == 2) {
            beanObject.put("else",  new ObjectWrapped(clauses.get(1)));
        }
        stack.push(beanObject);
    }

    @Override
    public void enterInputObjectlist(InputObjectlistContext ctx) {
        stack.push(StackMarker.ObjectList);
    }

    @Override
    public void exitInputObjectlist(InputObjectlistContext ctx) {
        List<ObjectDescription> l = new ArrayList<>();
        while(! StackMarker.ObjectList.equals(stack.peek())) {
            l.add((ObjectDescription) stack.pop());
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
        List<ObjectDescription> l = new ArrayList<>();
        while(! StackMarker.ObjectList.equals(stack.peek())) {
            l.add((ObjectDescription) stack.pop());
        }
        stack.pop();
        stack.push(l);
    }

    @Override
    public void exitOutput(OutputContext ctx) {
        PipeRefName piperef;
        @SuppressWarnings("unchecked")
        List<ObjectDescription> senders = (List<ObjectDescription>) stack.pop();
        if(stack.peek() != null && stack.peek() instanceof PipeRefName) {
            piperef = (PipeRefName) stack.pop();
        } else {
            // if no pipe name given, take events from the main pipe
            piperef = new PipeRefName("main");
        }
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
        List<ObjectDescription> receivers = (List<ObjectDescription>) stack.pop();
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
    public void exitSourcedef(SourcedefContext ctx) {
        String sourceName = ctx.Identifier().getText();
        ObjectDescription source = (ObjectDescription) stack.pop();
        sources.put(sourceName, source);
    }

    @Override
    public void enterArray(ArrayContext ctx) {
        stack.push(StackMarker.Array);
    }

    @Override
    public void exitArray(ArrayContext ctx) {
        List<Object> array = new ArrayList<>();
        while(! StackMarker.Array.equals(stack.peek()) ) {
            Object o = stack.pop();
            if(o instanceof ObjectWrapped) {
                o = ((ObjectWrapped) o).wrapped;
            }
            array.add(0, o);
        }
        stack.pop();
        stack.push(new ObjectWrapped(array.toArray()));
    }

    @Override
    public void exitDrop(DropContext ctx) {
        ObjectDescription drop = new ObjectDescription(this.stream, Drop.class.getCanonicalName(), ctx);
        stack.push(drop);
    }

    @Override
    public void enterFire(FireContext ctx) {
        stack.push(StackMarker.Fire);
    }

    @Override
    public void exitFire(FireContext ctx) {
        ObjectDescription fire = new ObjectDescription(this.stream, FireEvent.class.getName(), ctx);
        Map<String[], String> fields = new HashMap<>();
        int count = ctx.eventVariable().size() - 1;
        while(! StackMarker.Fire.equals(stack.peek()) ) {
            Object o = stack.pop();
            if(o instanceof ObjectWrapped) {
                String[] lvalue = convertEventVariable(ctx.eventVariable().get(count--));
                o = ((ObjectWrapped) o).wrapped;
                fields.put(lvalue, (String) o);
            } else if (o instanceof PipeRefName){
                PipeRefName name = (PipeRefName) o;
                fire.beans.put("destination", new ObjectWrapped(name.piperef));
            } else {
                throw new RecognitionException("invalid fire argument: " + o.toString(), parser, stream, ctx);
            }
        }
        fire.beans.put("fields", new ObjectWrapped(fields));
        stack.pop();
        stack.push(fire);
    }

    @Override
    public void exitLog(LogContext ctx) {
        ObjectDescription logger = new ObjectDescription(this.stream, Log.class.getName(), ctx);
        logger.beans.put("level", new ObjectWrapped(ctx.level().getText()));
        logger.beans.put("pipeName", new ObjectWrapped(currentPipeLineName));
        String message = ctx.message.getText();
        logger.beans.put("message", new ObjectWrapped(message));

        stack.push(logger);
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

        ObjectDescription etl;

        switch(ctx.op.getText()) {
        case("-"):
            etl = new ObjectDescription(this.stream, Etl.Remove.class.getName(), ctx);
        break;
        case("<"): {
            etl = new ObjectDescription(this.stream, Etl.Rename.class.getName(), ctx);
            etl.beans.put("source", new ObjectWrapped(convertEventVariable(ctx.eventVariable().get(1))));
            break;
        }
        case("="): {
            etl = new ObjectDescription(this.stream, Etl.Assign.class.getName(), ctx);
            ObjectWrapped expression = (ObjectWrapped) stack.pop();
            etl.beans.put("expression", expression);
            break;
        }
        case("("): {
            etl = new ObjectDescription(this.stream, Etl.Convert.class.getName(), ctx);
            ObjectWrapped className = new ObjectWrapped(ctx.QualifiedIdentifier().getText());
            etl.beans.put("className", className);
            break;
        }
        case("@"): {
            etl = new ObjectDescription(this.stream, Mapper.class.getName(), ctx);
            etl.beans.put("map", (ObjectReference) stack.pop());
            ObjectWrapped expression = (ObjectWrapped) stack.pop();
            etl.beans.put("expression", expression);
            break;
        }
        default:
            throw new RecognitionException("invalid operator " + ctx.op.getText(), parser, stream, ctx);
        }
        // Remove Etl marker
        Object o = stack.pop();
        assert StackMarker.Etl.equals(o);
        etl.beans.put("lvalue", new ConfigListener.ObjectWrapped(convertEventVariable(ctx.eventVariable().get(0))));
        stack.push(etl);
    }

    @Override
    public void enterMap(MapContext ctx) {
        stack.push(StackMarker.Map);
    }

    @Override
    public void exitMap(MapContext ctx) {
        if (ctx.source() == null) {
            Map<Object, Object> map = new HashMap<>();
            Object o;
            while((o = stack.pop()) != StackMarker.Map) { 
                ObjectWrapped value = (ObjectWrapped) o;
                ObjectWrapped key = (ObjectWrapped) stack.pop();
                map.put(key.wrapped, value.wrapped);
            };
            stack.push(new ConfigListener.ObjectWrapped(map));
        } else {
            assert stack.pop() == ConfigListener.StackMarker.Map;
            stack.push(new ConfigListener.Source(ctx.source().Identifier().getText(), sources));
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
            Arrays.stream(convertEventVariable(ctx.ev)).forEach( i-> {
                if (Event.TIMESTAMPKEY.equals(i)) {
                    buffer.append(".getTimestamp()");
                } else if (Event.CONTEXTKEY.equals(i)) {
                    buffer.append(".getConnectionContext()");
                } else {
                    buffer.append(".").append(i);
                }
            });
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
            stack.push( new ObjectWrapped(expression));
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
