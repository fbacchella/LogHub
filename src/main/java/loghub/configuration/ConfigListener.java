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
import loghub.Receiver;
import loghub.RouteBaseListener;
import loghub.RouteParser.BeanContext;
import loghub.RouteParser.BeanNameContext;
import loghub.RouteParser.BeanValueContext;
import loghub.RouteParser.InputContext;
import loghub.RouteParser.InputObjectlistContext;
import loghub.RouteParser.ObjectContext;
import loghub.RouteParser.OutputContext;
import loghub.RouteParser.OutputObjectlistContext;
import loghub.RouteParser.PipelineContext;
import loghub.RouteParser.PipenodeListContext;
import loghub.RouteParser.PiperefContext;
import loghub.RouteParser.TestContext;
import loghub.RouteParser.TestExpressionContext;
import loghub.Sender;
import loghub.Transformer;
import loghub.transformers.Pipe;
import loghub.transformers.PipeRef;
import loghub.transformers.Test;

import org.antlr.v4.runtime.tree.TerminalNode;

public class ConfigListener extends RouteBaseListener {

    private static enum StackMarker {
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

    public Deque<Object> stack = new ArrayDeque<>();

    public final Map<String, List<Pipe>> pipelines = new HashMap<>();
    public final List<Input> inputs = new ArrayList<>();
    public final List<Output> outputs = new ArrayList<>();

    private List<Pipe> currentPipeList = null;

    @Override
    public void enterPipeline(PipelineContext ctx) {
        String currentPipeLineName = ctx.Identifier().getText();
        currentPipeList = new ArrayList<>();
        pipelines.put(currentPipeLineName, currentPipeList);
    }

    @Override
    public void exitPipeline(PipelineContext ctx) {
        stack.pop();
    }

    @Override
    public void enterBeanName(BeanNameContext ctx) {
        stack.push(ctx.getText());
    }

    @Override
    public void enterBeanValue(BeanValueContext ctx) {
        TerminalNode literal = ctx.Literal();

        // Only needed to push if Literal
        // Overwise the object will be pushed anyway
        if(literal != null) {
            stack.push(ctx.getText());
        }
    }

    @Override
    public void exitBean(BeanContext ctx) {
        Object beanValue = stack.pop();
        String beanName = (String) stack.pop();
        Object beanObject = stack.peek();
        PropertyDescriptor bean;
        try {
            bean = new PropertyDescriptor(beanName, beanObject.getClass());
        } catch (IntrospectionException e1) {
            throw new ConfigException(String.format("Unknown bean '%s'", beanName), ctx.start, ctx.stop, e1);
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
            } else {
                throw new ConfigException(String.format("Invalid internal stack state for '%s'", beanName), ctx.start, ctx.stop);                
            }
        } catch (IllegalAccessException|IllegalArgumentException|InvocationTargetException e) {
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
                | NoSuchMethodException | SecurityException e) {
            throw new ConfigException(String.format("Invalid class '%s'", qualifiedName), ctx.start, ctx.stop);
        }
        stack.push(beanObject);
    }

    @Override
    public void enterPipenodeList(PipenodeListContext ctx) {
        stack.push(StackMarker.PipeNodeList );
    }

    @Override
    public void exitPiperef(PiperefContext ctx) {
        stack.push(ctx.Identifier().getText());
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
            if(stack.peek().getClass().isAssignableFrom(String.class)) {
                PipeRef piperef = new PipeRef();
                piperef.setPipeRef((String) stack.pop());
                stack.push(piperef);
            }
            Transformer t = (Transformer) stack.pop();
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
        }
        //Remove the marker
        stack.pop();
        Pipe pipe = new Pipe();
        pipe.setPipe(pipeList);
        stack.push(pipe);
        currentPipeList.add(pipe);
    }

    @Override
    public void enterTestExpression(TestExpressionContext ctx) {
        stack.push(ctx.getText());
    }

    @Override
    public void exitTest(TestContext ctx) {
        Test testTransformer = new Test();
        Object o2 = stack.pop();
        Object o1 = stack.pop();
        String test;
        if(Transformer.class.isAssignableFrom(o1.getClass())) {
            testTransformer.setElse((Transformer) o2);
            testTransformer.setThen((Transformer) o1);
            test = (String) stack.pop();
        }
        else {
            test = (String) o1;
            testTransformer.setThen((Transformer) o2);
        }
        testTransformer.setIf(test);
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
        String piperef = null;
        @SuppressWarnings("unchecked")
        List<Sender> senders = (List<Sender>) stack.pop();
        if(stack.peek() != null && stack.peek().getClass().isAssignableFrom(String.class)) {
            piperef = (String) stack.pop();
        }
        Output output = new Output(senders, piperef);
        outputs.add(output);
    }

    @Override
    public void exitInput(InputContext ctx) {
        String piperef = null;
        if(stack.peek().getClass().isAssignableFrom(String.class)) {
            piperef = (String) stack.pop();
        }
        @SuppressWarnings("unchecked")
        List<Receiver> receivers = (List<Receiver>) stack.pop();
        Input input = new Input(receivers, piperef);
        inputs.add(input);
    }

}
