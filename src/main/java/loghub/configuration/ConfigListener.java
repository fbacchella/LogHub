package loghub.configuration;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import loghub.PipeStep;
import loghub.Pipeline;
import loghub.Receiver;
import loghub.RouteBaseListener;
import loghub.RouteParser.BeanContext;
import loghub.RouteParser.BeanNameContext;
import loghub.RouteParser.BooleanLiteralContext;
import loghub.RouteParser.CharacterLiteralContext;
import loghub.RouteParser.FloatingPointLiteralContext;
import loghub.RouteParser.InputContext;
import loghub.RouteParser.InputObjectlistContext;
import loghub.RouteParser.IntegerLiteralContext;
import loghub.RouteParser.ObjectContext;
import loghub.RouteParser.OutputContext;
import loghub.RouteParser.OutputObjectlistContext;
import loghub.RouteParser.PipelineContext;
import loghub.RouteParser.PipenodeListContext;
import loghub.RouteParser.PiperefContext;
import loghub.RouteParser.PropertyContext;
import loghub.RouteParser.StringLiteralContext;
import loghub.RouteParser.TestContext;
import loghub.RouteParser.TestExpressionContext;
import loghub.Sender;
import loghub.Transformer;
import loghub.transformers.PipeRef;
import loghub.transformers.Test;

class ConfigListener extends RouteBaseListener {

    private static enum StackMarker {
        Test,
        ObjectList,
        PipeNodeList;
        boolean isEquals(Object other) {
            return (other != null && other instanceof StackMarker && equals(other));
        }
    };

    final class Input {
        final List<Receiver> receiver;
        String piperef;
        Input(List<Receiver>receiver, String piperef) {
            this.piperef = piperef;
            this.receiver = receiver;
        }
        @Override
        public String toString() {
            return "(" + receiver.toString() + " -> " + piperef + ")";
        }
    }

    final class Output {
        final List<Sender> sender;
        final String piperef;
        Output(List<Sender>sender, String piperef) {
            this.piperef = piperef;
            this.sender = sender;
        }
        @Override
        public String toString() {
            return "(" + piperef + " -> " +  sender.toString() + ")";
        }
    }

    final class PipeRefName {
        final String piperef;
        private PipeRefName(String piperef) {
            this.piperef = piperef;
        }
    }

    Deque<Object> stack = new ArrayDeque<>();

    final Map<String, List<Pipeline>> pipelines = new HashMap<>();
    final List<Input> inputs = new ArrayList<>();
    final List<Output> outputs = new ArrayList<>();
    final Map<String, Object> properties = new HashMap<>();

    private List<Pipeline> currentPipeList = null;
    private String currentPipeLineName = null;

    @Override
    public void enterPiperef(PiperefContext ctx) {
        stack.push(new PipeRefName(ctx.getText()));
    }

    @Override
    public void enterBeanName(BeanNameContext ctx) {
        stack.push(ctx.getText());
    }

    @Override
    public void enterFloatingPointLiteral(FloatingPointLiteralContext ctx) {
        String content = ctx.FloatingPointLiteral().getText();
        stack.push(new Double(content));
    }

    @Override
    public void enterCharacterLiteral(CharacterLiteralContext ctx) {
        String content = ctx.CharacterLiteral().getText();
        stack.push(content.charAt(0));
    }

    @Override
    public void enterStringLiteral(StringLiteralContext ctx) {
        String content = ctx.StringLiteral().getText();
        // remove the wrapping "..."
        stack.push(content.substring(1, content.length() - 1));
    }

    @Override
    public void enterIntegerLiteral(IntegerLiteralContext ctx) {
        String content = ctx.IntegerLiteral().getText();
        stack.push(new Integer(content));
    }

    @Override
    public void enterBooleanLiteral(BooleanLiteralContext ctx) {
        String content = ctx.getText();
        stack.push(new Boolean(content));
    }

    @Override
    public void exitBean(BeanContext ctx) {
        Object beanValue = stack.pop();
        String beanName = (String) stack.pop();
        Object beanObject = stack.peek();
        PropertyDescriptor bean;
        try {
            bean = new PropertyDescriptor(beanName, beanObject.getClass());
        } catch (IntrospectionException e) {
            throw new ConfigException(String.format("Unknown bean '%s'", beanName), ctx.start, ctx.stop, e);
        }

        Method setMethod = bean.getWriteMethod();
        if(setMethod == null) {
            throw new ConfigException(String.format("Unknown bean '%s'", beanName), ctx.start, ctx.stop);
        }
        Class<?> setArgType = bean.getPropertyType();
        try {
            if(setArgType.isAssignableFrom(beanValue.getClass())) {
                setMethod.invoke(beanObject, beanValue);                       
            } else if (beanValue instanceof String){
                Object argInstance = BeansManager.ConstructFromString(setArgType, (String) beanValue);
                setMethod.invoke(beanObject, argInstance);                       
            } else if (beanValue instanceof Number){
                setMethod.invoke(beanObject, beanValue);                       
            } else {
                throw new ConfigException(String.format("Invalid internal stack state for '%s'", beanName), ctx.start, ctx.stop);                
            }
        } catch (InvocationTargetException e) {
            throw new ConfigException(String.format("Bean setter failed for '%s': %s", beanName, e.getCause().getMessage()), ctx.start, ctx.stop);
        } catch (IllegalAccessException|IllegalArgumentException e) {
            throw new ConfigException(String.format("Invalid bean setter for '%s'", beanName), ctx.start, ctx.stop);
        }
    }

    @Override
    public void enterObject(ObjectContext ctx) {
        String qualifiedName = ctx.QualifiedIdentifier().getText();
        Class<?> clazz;
        try {
            clazz = getClass().getClassLoader().loadClass(qualifiedName);
        } catch (ClassNotFoundException e) {
            throw new ConfigException(String.format("Unknown class '%s'", qualifiedName), ctx.start, ctx.stop);
        }
        Object beanObject;
        try {
            beanObject = clazz.getConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException
                | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException | ExceptionInInitializerError e) {
            throw new ConfigException(String.format("Invalid class '%s': %s", qualifiedName), ctx.start, ctx.stop);
        }
        stack.push(beanObject);
    }

    @Override
    public void enterPipeline(PipelineContext ctx) {
        currentPipeLineName = ctx.Identifier().getText();
        currentPipeList = new ArrayList<>();
        pipelines.put(currentPipeLineName, currentPipeList);
    }

    @Override
    public void exitPipeline(PipelineContext ctx) {
        stack.pop();
        currentPipeLineName = null;
    }

    @Override
    public void enterPipenodeList(PipenodeListContext ctx) {
        stack.push(StackMarker.PipeNodeList );
    }

    @Override
    public void exitPipenodeList(PipenodeListContext ctx) {
        List<PipeStep[]> pipeList = new ArrayList<PipeStep[]>() {
            @Override
            public String toString() {
                StringBuilder buffer = new StringBuilder();
                buffer.append("PipeList(");
                for(PipeStep[] i: this) {
                    buffer.append(Arrays.toString(i));
                    buffer.append(", ");
                }
                buffer.setLength(buffer.length() - 2);
                buffer.append(')');
                return buffer.toString();
            }
        };
        int rank = 0;
        int threads = -1;
        PipeStep[] step = null;
        while( ! StackMarker.PipeNodeList.isEquals(stack.peek()) ) {
            Object poped = stack.pop();
            // A pipe transformer provides is own PipeStep
            if(poped instanceof Pipeline) {
                Pipeline pipeline = (Pipeline) poped;
                // the pipestep can't be reused
                threads = -1;
                pipeList.add(pipeline.getPipeSteps());
            } else if(poped instanceof Transformer){
                Transformer t = (Transformer) poped;
                if(t.getThreads() != threads) {
                    threads = t.getThreads();
                    step = new PipeStep[threads];
                    pipeList.add(step);
                    rank++;
                    for(int i=0; i < threads ; i++) {
                        step[i] = new PipeStep(rank, i + 1);
                    }
                }
                for(int i = 0; i < threads ; i++) {
                    step[i].addTransformer(t);
                }                
            } else {
                throw new ConfigException("unknown stack state " + poped, ctx.start, ctx.stop);
            }
        }
        //Remove the marker
        stack.pop();
        Pipeline pipe = new Pipeline(pipeList, currentPipeLineName + "$" + currentPipeList.size());
        stack.push(pipe);
        currentPipeList.add(pipe);
    }

    @Override
    public void exitPiperef(PiperefContext ctx) {
        // In pipenode, part of a pipeline, expect to find a transformer, so transform the name to a PipeRef transformer
        // Other case the name is kept as is
        if(ctx.getParent() instanceof loghub.RouteParser.PipenodeContext) {
            PipeRef piperef = new PipeRef();
            piperef.setPipeRef(((PipeRefName) stack.pop()).piperef);
            stack.push(piperef);
        }
    }

    @Override
    public void enterTestExpression(TestExpressionContext ctx) {
        stack.push(StackMarker.Test);
    }

    @Override
    public void exitTest(TestContext ctx) {
        Test testTransformer = new Test();
        PipeStep[][] clauses = new PipeStep[2][];

        for(int i=1; !( stack.peek() instanceof StackMarker) ; i-- ) {
            Object o = stack.pop();
            if(o instanceof Pipeline) {
                Pipeline p = (Pipeline) o;
                clauses[i] =  p.getPipeSteps();
            } else if (o instanceof PipeStep[] ) {
                PipeStep[] p = (PipeStep[]) o;
                clauses[i] = p;
            } else if (o instanceof Transformer ) {
                Transformer t = (Transformer) o;
                PipeStep[] steps = new PipeStep[t.getThreads()];
                for(int j = 0; j < steps.length ; j++) {
                    steps[j] = new PipeStep();
                    steps[j].addTransformer(t);
                }
                clauses[i] = steps;
            }
        };
        stack.pop();
        testTransformer.setIf(ctx.testExpression().getText());
        stack.push(testTransformer);
    }

    @Override
    public void enterInputObjectlist(InputObjectlistContext ctx) {
        stack.push(StackMarker.ObjectList);
    }

    @Override
    public void exitInputObjectlist(InputObjectlistContext ctx) {
        List<Receiver> l = new ArrayList<>();
        while(! StackMarker.ObjectList.equals(stack.peek())) {
            l.add((Receiver) stack.pop());
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
        List<Sender> l = new ArrayList<>();
        while(! StackMarker.ObjectList.equals(stack.peek())) {
            l.add((Sender) stack.pop());
        }
        stack.pop();
        stack.push(l);
    }

    @Override
    public void exitOutput(OutputContext ctx) {
        PipeRefName piperef = new PipeRefName("main");
        @SuppressWarnings("unchecked")
        List<Sender> senders = (List<Sender>) stack.pop();
        if(stack.peek() != null && stack.peek() instanceof PipeRefName) {
            piperef = (PipeRefName) stack.pop();
        }
        Output output = new Output(senders, piperef.piperef);
        outputs.add(output);
    }

    @Override
    public void exitInput(InputContext ctx) {
        PipeRefName piperef;
        if(stack.peek() instanceof PipeRefName) {
            piperef = (PipeRefName) stack.pop();
        } else {
            // if no pipe name given, data are sent to the main pipe
            piperef = new PipeRefName("main");
        }
        @SuppressWarnings("unchecked")
        List<Receiver> receivers = (List<Receiver>) stack.pop();
        Input input = new Input(receivers, piperef.piperef);
        inputs.add(input);
    }

    @Override
    public void exitProperty(PropertyContext ctx) {
        Object value = stack.pop();
        String key = ctx.Identifier().getText();
        properties.put(key, value);
    }

}
