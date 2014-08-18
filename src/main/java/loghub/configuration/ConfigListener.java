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

import org.antlr.v4.runtime.Token;

import loghub.PipeStep;
import loghub.RouteBaseListener;
import loghub.RouteParser.BeanContext;
import loghub.RouteParser.BeanNameContext;
import loghub.RouteParser.BeanValueContext;
import loghub.RouteParser.ObjectContext;
import loghub.RouteParser.PipelineContext;
import loghub.RouteParser.PipenodeListContext;
import loghub.RouteParser.TestContext;
import loghub.RouteParser.TestExpressionContext;
import loghub.Transformer;
import loghub.transformers.Pipe;
import loghub.transformers.Test;

public class ConfigListener extends RouteBaseListener {

    public final class ConfigException extends RuntimeException {
        private final Token start;
        private final Token end;
        ConfigException(String message, Token start, Token end, Exception e) {
            super(message, e);
            this.start = start;
            this.end = end;
        }
        ConfigException(String message, Token start, Token end) {
            super(message);
            this.start = start;
            this.end = end;
        }
        public String getStartPost() {
            return "line " + start.getLine() + ":" + start.getCharPositionInLine();
        }
        public String getStartEnd() {
            return "line " + end.getLine() + ":" + end.getCharPositionInLine();
        }
    };
    public Deque<Object> stack = new ArrayDeque<>();

    public final Map<String, Pipe> pipelines = new HashMap<>();

    private static final class PipeNodeList {};

    @Override
    public void exitPipeline(PipelineContext ctx) {
        System.out.println(stack);
        System.out.println(ctx.start);
        System.out.println(ctx.stop);
        String name = ctx.Identifier().getText();
        Pipe pipe = (Pipe) stack.pop();
        pipelines.put(name, pipe);
    }

    @Override
    public void enterBeanName(BeanNameContext ctx) {
        stack.push(ctx.getText());
    }

    @Override
    public void enterBeanValue(BeanValueContext ctx) {
        stack.push(ctx.getText());
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
            throw new ConfigException(String.format("Unknown bean '%s'", beanName), ctx.start, ctx.stop);
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
        stack.push(new PipeNodeList() );
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
        while( ! (stack.peek() instanceof PipeNodeList)) {
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

}
