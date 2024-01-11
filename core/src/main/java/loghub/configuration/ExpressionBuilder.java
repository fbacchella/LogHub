package loghub.configuration;

import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import loghub.Expression;
import loghub.ProcessorException;
import loghub.VarFormatter;
import loghub.VariablePath;
import lombok.Getter;

class ExpressionBuilder {

    enum ExpressionType {
        LITERAL,
        FORMATTER,
        VARPATH,
        LAMBDA,
    }

    @Getter
    private ExpressionType type;
    private Object payload;

    private ExpressionBuilder() {
    }

    @SuppressWarnings("unchecked")
    public <T> T getPayload() {
        return (T) payload;
    }

    ExpressionBuilder setPayload(Object l) {
        this.payload = l;
        return this;
    }

    public ExpressionBuilder setType(ExpressionType type) {
        this.type = type;
        return this;
    }

    Expression build(String source) {
        switch (type) {
        case LAMBDA:
            return new Expression(source, (Expression.ExpressionLambda) payload);
        case VARPATH:
            return new Expression(source, (VariablePath) payload);
        case LITERAL:
            return new Expression(source, payload);
        case FORMATTER:
            return new Expression(source, (VarFormatter) payload);
        default:
            throw new UnsupportedOperationException("Unreachable " + type);
        }
    }

    private Expression.ExpressionLambda asLambda() {
        switch (type) {
        case LITERAL:
            return ed -> payload;
        case LAMBDA:
            return getPayload();
        case VARPATH:
            VariablePath vp = getPayload();
            return ed -> Expression.nullfilter(ed.getEvent().getAtPath(vp));
        case FORMATTER:
            VarFormatter vf = getPayload();
            return ed -> vf.format(ed.getEvent());
        default:
            throw new UnsupportedOperationException("Unreachable " + type);
        }
    }

    private ExpressionBuilder setPayload(List<ExpressionBuilder> list) {
        List<Expression.ExpressionLambda> lambdas = list.stream().map(ExpressionBuilder::asLambda).filter(Objects::nonNull).collect(Collectors.toList());
        this.payload = (Expression.ExpressionLambda)ed -> lambdas.stream().map(l -> l.apply(ed)).collect(Collectors.toList());
        return this;
    }

    public static ExpressionBuilder of(VarFormatter vf) {
        if (vf.isEmpty()) {
            return new ExpressionBuilder().setType(ExpressionType.LITERAL).setPayload(vf.argsFormat());
        } else {
            return new ExpressionBuilder().setType(ExpressionType.FORMATTER).setPayload(vf);
        }
    }

    public static ExpressionBuilder of(Object literal) {
        return new ExpressionBuilder().setType(ExpressionType.LITERAL).setPayload(literal);
    }

    public static ExpressionBuilder of(VariablePath vp) {
        return new ExpressionBuilder().setType(ExpressionType.VARPATH).setPayload(vp);
    }

    public static ExpressionBuilder of(List<ExpressionBuilder> expressions) {
        return new ExpressionBuilder().setType(ExpressionType.LAMBDA).setPayload(expressions);
    }

    public static ExpressionBuilder of(ExpressionBuilder pre, String op, ExpressionBuilder post) {
        Expression.ExpressionLambda l1 = pre.asLambda();
        Expression.ExpressionLambda l2 = post.asLambda();
        Expression.ExpressionLambda lambda = ed -> Expression.groovyOperator(op, l1.apply(ed), l2.apply(ed));
        if (pre.type == ExpressionType.LITERAL && post.type == ExpressionType.LITERAL) {
            return evalStatic(lambda);
        } else {
            return new ExpressionBuilder().setType(ExpressionType.LAMBDA).setPayload(lambda);
        }
    }

    public static ExpressionBuilder of(Expression.ExpressionLambda l) {
        return new ExpressionBuilder().setType(ExpressionType.LAMBDA).setPayload(l);
    }

    @SuppressWarnings("unchecked")
    public static <T, R> ExpressionBuilder of(ExpressionBuilder exp, Function<T, R> f) {
        if (exp.type == ExpressionType.LITERAL) {
            return new ExpressionBuilder().setType(ExpressionType.LITERAL).setPayload(f.apply((T) exp.payload));
        } else {
            Expression.ExpressionLambda l = ed -> f.apply((T) exp.asLambda().apply(ed));
            return new ExpressionBuilder().setType(ExpressionType.LAMBDA).setPayload(l);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T, U, R> ExpressionBuilder of(ExpressionBuilder exp1, ExpressionBuilder exp2, BiFunction<T, U, R> f) {
        if (exp1.type == ExpressionType.LITERAL && exp2.type == ExpressionType.LITERAL ) {
            return new ExpressionBuilder().setType(ExpressionType.LITERAL).setPayload(f.apply((T) exp1.payload, (U) exp2.payload));
        } else {
            Expression.ExpressionLambda l = ed -> f.apply((T) exp1.asLambda().apply(ed), (U) exp2.asLambda().apply(ed));
            return new ExpressionBuilder().setType(ExpressionType.LAMBDA).setPayload(l);
        }
    }

    private static ExpressionBuilder evalStatic(Expression.ExpressionLambda lambda) {
        try {
            return new ExpressionBuilder().setType(ExpressionType.LITERAL).setPayload(new Expression("", lambda).eval());
        } catch (ProcessorException e) {
            throw new IllegalArgumentException(e);
        }
    }

}
