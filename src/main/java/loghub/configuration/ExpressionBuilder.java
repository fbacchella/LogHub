package loghub.configuration;

import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import loghub.Expression;
import loghub.Helpers;
import loghub.NullOrMissingValue;
import loghub.VarFormatter;
import loghub.VariablePath;
import lombok.Getter;

class ExpressionBuilder {

    enum ExpressionType {
        CONSTANT,
        LITERAL,
        VARIABLE,
        OPERATOR,
        FORMATTER,
        VARPATH,
        LAMBDA
    }

    @Getter
    private ExpressionType type;
    private Object payload;
    private ExpressionBuilder previous = null;

    ExpressionBuilder() {
    }

    @SuppressWarnings("unchecked")
    public <T> T getPayload() {
        return (T) payload;
    }

    public ExpressionBuilder snap() {
        ExpressionBuilder next = new ExpressionBuilder();
        next.type = this.type;
        next.payload = this.payload;
        next.previous = this;
        return next;
    }

    ExpressionBuilder setNull() {
        this.payload = NullOrMissingValue.NULL;
        this.type = ExpressionType.LITERAL;
        return this;
    }

    ExpressionBuilder setCharacter(Character c) {
        this.payload = c;
        this.type = ExpressionType.LITERAL;
        return this;
    }

    ExpressionBuilder setPayload(Object l) {
        this.payload = l;
        this.type = ExpressionType.LITERAL;
        return this;
    }

    public ExpressionBuilder setVariablePath(VariablePath path) {
        type = ExpressionType.VARPATH;
        payload = path;
        return this;
    }

    public ExpressionBuilder setType(ExpressionType type) {
        this.type = type;
        return this;
    }

    ExpressionBuilder join(ExpressionType newType) {
        if (newType == ExpressionType.LAMBDA) {
            type = ExpressionType.LAMBDA;
        } else if (previous == null) {
            type = newType;
        } else if (newType == ExpressionType.VARIABLE || previous.type == ExpressionType.VARPATH) {
            type = ExpressionType.VARIABLE;
        } else if (newType == ExpressionType.OPERATOR && previous.type == ExpressionType.LITERAL) {
            type = ExpressionType.CONSTANT;
        }
        return this;
    }

    ExpressionBuilder merge(ExpressionBuilder exi1, ExpressionBuilder exi2) {
        ExpressionType type1 = exi1.type;
        ExpressionType type2 = exi2.type;
        if (type1 == ExpressionType.VARIABLE || type2 == ExpressionType.VARIABLE || type1 == ExpressionType.VARPATH || type2 == ExpressionType.VARPATH || type1 == ExpressionType.LAMBDA || type2 == ExpressionType.LAMBDA) {
            type = ExpressionType.VARIABLE;
        } else {
            type = ExpressionType.CONSTANT;
        }
        return this;
    }

    public ExpressionBuilder setLambda(Expression.ExpressionLambda lambda) {
        type = ExpressionType.LAMBDA;
        this.payload = lambda;
        return this;
    }


    public ExpressionBuilder setLambda(ExpressionBuilder subexpression, BiFunction<Expression.ExpressionLambda, Expression.ExpressionData, Object> lambda) {
        Expression.ExpressionLambda sublambda = subexpression.asLambda();
        if (sublambda != null) {
            setLambda(ed -> lambda.apply(sublambda, ed));
        } else {
            join(ExpressionType.OPERATOR);
        }
        return this;
    }

    public ExpressionBuilder setLambda(ExpressionBuilder subexpression1, ExpressionBuilder subexpression2, Helpers.TriFunction<Expression.ExpressionLambda, Expression.ExpressionLambda, Expression.ExpressionData, Object> lambda) {
        Expression.ExpressionLambda sublambda1 = subexpression1.asLambda();
        Expression.ExpressionLambda sublambda2 = subexpression2.asLambda();
        if (sublambda1 != null && sublambda2 != null) {
            setLambda(ed -> lambda.apply(sublambda1, sublambda2, ed));
        } else {
            join(ExpressionType.OPERATOR);
        }
        return this;
    }

    public ExpressionBuilder setOperator() {
        join(ExpressionType.OPERATOR);
        return this;
    }

    public ExpressionBuilder setBiOperator(ExpressionBuilder exp1, ExpressionBuilder exp2) {
        merge(exp1, exp2);
        return this;
    }


    ExpressionBuilder binaryInfixOperator(ExpressionBuilder pre, String op, ExpressionBuilder post) {
        merge(pre, post);
        setLambda(pre, post, (l1, l2, ed) -> ed.getExpression().groovyOperator(op, l1.apply(ed), l2.apply(ed)));
        return this;
    }

    public ExpressionBuilder setVarFormatter(VarFormatter vf) {
        this.type = ExpressionType.FORMATTER;
        this.payload = vf;
        return this;
    }

    ExpressionBuilder getExpressionList(List<ExpressionBuilder> expressions) {
        ExpressionBuilder.ExpressionType nextType = ExpressionBuilder.ExpressionType.CONSTANT;
        for (ExpressionBuilder exinfo: expressions) {
            if (exinfo.getType() == ExpressionBuilder.ExpressionType.VARIABLE ||
                        exinfo.getType() == ExpressionBuilder.ExpressionType.VARPATH ||
                        exinfo.getType() == ExpressionBuilder.ExpressionType.LAMBDA) {
                nextType = ExpressionBuilder.ExpressionType.VARIABLE;
            }
        }
        setType(nextType);
        payload = fromList(expressions);
        if (payload != null) {
            type = ExpressionType.LAMBDA;
        }
        return this;
    }

    Expression build() {
        switch (type) {
        case LAMBDA:
        case VARPATH:
        case LITERAL:
            return new Expression(payload);
        case FORMATTER:
            return new Expression(payload);
        default:
            throw new UnsupportedOperationException("Unreachable");
        }
    }

    Expression.ExpressionLambda asLambda() {
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
        case CONSTANT:
            throw new UnsupportedOperationException("Unreachable");
        case OPERATOR:
        case VARIABLE:
        default:
            return null;
        }
    }

    private Expression.ExpressionLambda fromList(List<ExpressionBuilder> list) {
        List<Expression.ExpressionLambda> lambdas = list.stream().map(ExpressionBuilder::asLambda).filter(Objects::nonNull).collect(Collectors.toList());
        if (list.size() != lambdas.size()) {
            // one of the lambda was null, so the whole list is not usable.
            return null;
        } else {
            return ed -> lambdas.stream().map(l -> l.apply(ed)).collect(Collectors.toList());
        }
    }

}
