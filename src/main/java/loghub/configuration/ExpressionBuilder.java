package loghub.configuration;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import loghub.Expression;
import loghub.VarFormatter;
import loghub.VariablePath;
import lombok.Getter;

class ExpressionBuilder {

    public interface BiFunction {
        Object eval(Expression.ExpressionData ed, Expression.ExpressionLambda l1, Expression.ExpressionLambda l2);
    }

    public interface Function {
        Object eval(Expression.ExpressionData ed, Expression.ExpressionLambda l);
    }

    enum ExpressionType {
        LITERAL,
        FORMATTER,
        VARPATH,
        LAMBDA
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
            return ed -> ed.getExpression().nullfilter(ed.getEvent().getAtPath(vp));
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
        return new ExpressionBuilder().setType(ExpressionType.FORMATTER).setPayload(vf);
    }

    public static ExpressionBuilder of(Object litteral) {
        return new ExpressionBuilder().setType(ExpressionType.LITERAL).setPayload(litteral);
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
        Expression.ExpressionLambda lambda = ed -> ed.getExpression().groovyOperator(op, l1.apply(ed), l2.apply(ed));
        return new ExpressionBuilder().setType(ExpressionType.LAMBDA).setPayload(lambda);
    }

    public static ExpressionBuilder of(Expression.ExpressionLambda l) {
        return new ExpressionBuilder().setType(ExpressionType.LAMBDA).setPayload(l);
    }

    public static ExpressionBuilder of(ExpressionBuilder pre, ExpressionBuilder post, BiFunction f) {
        Expression.ExpressionLambda l1 = pre.asLambda();
        Expression.ExpressionLambda l2 = post.asLambda();
        Expression.ExpressionLambda l = ed -> f.eval(ed, l1, l2);
        return new ExpressionBuilder().setType(ExpressionType.LAMBDA).setPayload(l);
    }

    public static ExpressionBuilder of(ExpressionBuilder exp, Function f) {
        Expression.ExpressionLambda l = ed -> f.eval(ed, exp.asLambda());
        return new ExpressionBuilder().setType(ExpressionType.LAMBDA).setPayload(l);
    }

}
