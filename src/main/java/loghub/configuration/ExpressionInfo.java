package loghub.configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.BiFunction;

import org.antlr.v4.runtime.ParserRuleContext;

import loghub.Expression;
import loghub.Helpers;
import loghub.NullOrMissingValue;
import loghub.ProcessorException;
import loghub.VarFormatter;
import loghub.VariablePath;
import lombok.Getter;

class ExpressionInfo {

    enum ExpressionType {
        CONSTANT,
        LITTERAL,
        VARIABLE,
        OPERATOR,
        FORMATTER,
        VARPATH,
        LAMBDA
    }

    private final ConfigListener configListener;
    @Getter
    private String expression;
    @Getter
    private ExpressionType type;
    private Object payload;
    private ExpressionInfo previous = null;

    static ExpressionInfo of(ConfigListener configListener) {
        return new ExpressionInfo(configListener);
    }

    private ExpressionInfo(ConfigListener configListener) {
        this.configListener = configListener;
    }

    public <T> T getPayload() {
        return (T) payload;
    }

    public ExpressionInfo join(String newExpression, ExpressionType newType) {
        expression = newExpression;
        return this.join(newType);
    }

    public ExpressionInfo snap() {
        ExpressionInfo next = new ExpressionInfo(configListener);
        next.expression = this.expression;
        next.type = this.type;
        next.payload = this.payload;
        next.previous = this;
        return next;
    }

    ExpressionInfo setNull() {
        this.expression = "loghub.NullOrMissingValue.NULL";
        this.payload = NullOrMissingValue.NULL;
        this.type = ExpressionType.LITTERAL;
        return this;
    }

    ExpressionInfo setCharacter(Character c) {
        this.expression = String.format("('%s' as char)", c);
        this.payload = c;
        this.type = ExpressionType.LITTERAL;
        return this;
    }

    ExpressionInfo setPayload(Object l) {
        this.payload = l;
        this.type = ExpressionType.LITTERAL;
        return this;
    }

    ExpressionInfo setExpression(String format, Object... args) {
        for (int i=0; i < args.length; i++) {
            if (args[i] instanceof  ExpressionInfo) {
                args[i] = ((ExpressionInfo)args[i] ).expression;
            }
        }
        expression = String.format(Locale.ENGLISH, format, args);
        return this;
    }

    public ExpressionInfo setVariablePath(VariablePath path) {
        expression = path.groovyExpression();
        type = ExpressionType.VARPATH;
        payload = path;
        return this;
    }

    public ExpressionInfo setType(ExpressionType type) {
        this.type = type;
        return this;
    }

    ExpressionInfo join(ExpressionType newType) {
        if (newType == ExpressionType.LAMBDA) {
            type = ExpressionType.LAMBDA;
        } else if (previous == null) {
            type = newType;
        } else if (newType == ExpressionType.VARIABLE || previous.type == ExpressionType.VARPATH) {
            type = ExpressionType.VARIABLE;
        } else if (newType == ExpressionType.OPERATOR && previous.type == ExpressionType.LITTERAL) {
            type = ExpressionType.CONSTANT;
        }
        return this;
    }

    ExpressionInfo merge(ExpressionInfo exi1, ExpressionInfo exi2) {
        ExpressionType type1 = exi1.type;
        ExpressionType type2 = exi2.type;
        if (type1 == ExpressionType.VARIABLE || type2 == ExpressionType.VARIABLE || type1 == ExpressionType.VARPATH || type2 == ExpressionType.VARPATH || type1 == ExpressionType.LAMBDA || type2 == ExpressionType.LAMBDA) {
            type = ExpressionType.VARIABLE;
        } else {
            type = ExpressionType.CONSTANT;
        }
        return this;
    }

    public ExpressionInfo setLambda(Expression.ExpressionLambda lambda) {
        type = ExpressionType.LAMBDA;
        this.payload = lambda;
        return this;
    }


    public ExpressionInfo setLambda(ExpressionInfo subexpression, BiFunction<Expression.ExpressionLambda, Expression.ExpressionData, Object> lambda) {
        Expression.ExpressionLambda sublambda = subexpression.asLambda();
        if (sublambda != null) {
            setLambda(ed -> lambda.apply(sublambda, ed));
        } else {
            join(ExpressionType.OPERATOR);
        }
        return this;
    }

    public ExpressionInfo setLambda(ExpressionInfo subexpression1, ExpressionInfo subexpression2, Helpers.TriFunction<Expression.ExpressionLambda, Expression.ExpressionLambda, Expression.ExpressionData, Object> lambda) {
        Expression.ExpressionLambda sublambda1 = subexpression1.asLambda();
        Expression.ExpressionLambda sublambda2 = subexpression2.asLambda();
        if (sublambda1 != null && sublambda2 != null) {
            setLambda(ed -> lambda.apply(sublambda1, sublambda2, ed));
        } else {
            join(ExpressionType.OPERATOR);
        }
        return this;
    }

    public ExpressionInfo setOperator(String format, Object... args) {
        setExpression(format, args);
        join(ExpressionType.OPERATOR);
        return this;
    }

    public ExpressionInfo setBiOperator(String format, ExpressionInfo exp1, ExpressionInfo exp2) {
        setExpression(format, exp1, exp2);
        merge(exp1, exp2);
        return this;
    }

    public ExpressionInfo setBiOperator(String format, String operator, ExpressionInfo exp1, ExpressionInfo exp2) {
        setExpression(format, operator, exp1, exp2);
        merge(exp1, exp2);
        return this;
    }


    ExpressionInfo binaryInfixOperator(ExpressionInfo pre, String op, ExpressionInfo post) {
        String preStringFormat;
        if (pre.type == ExpressionType.LITTERAL && pre.payload != null) {
            preStringFormat = "(%s)";
        } else {
            preStringFormat = "ex.nullfilter(%s)";
        }
        String postStringFormat;
        String postOp;
        if (post.type == ExpressionType.LITTERAL && post.payload != null) {
            postStringFormat = "%s(%s)";
            postOp = "";
        } else {
            postStringFormat = "ex.protect(\"%s\", %s)";
            postOp = op;
        }
        setExpression(preStringFormat + " %s " + postStringFormat, pre, op, postOp, post);
        merge(pre, post);
        setLambda(pre, post, (l1, l2, ed) -> ed.getExpression().groovyOperator(op, l1.apply(ed), l2.apply(ed)));
        return this;
    }

    public ExpressionInfo setVarFormatter(VarFormatter vf) {
        this.type = ExpressionType.FORMATTER;
        this.payload = vf;
        return this;
    }

    ExpressionInfo getExpressionList(List<ExpressionInfo> expressions) {
        StringBuilder exlist = new StringBuilder();
        ExpressionInfo.ExpressionType nextType = ExpressionInfo.ExpressionType.CONSTANT;
        for (ExpressionInfo exinfo: expressions) {
            if (exinfo.getType() == ExpressionInfo.ExpressionType.VARIABLE ||
                        exinfo.getType() == ExpressionInfo.ExpressionType.VARPATH ||
                        exinfo.getType() == ExpressionInfo.ExpressionType.LAMBDA) {
                nextType = ExpressionInfo.ExpressionType.VARIABLE;
            }
            exlist.append(exlist.length() == 0 ? "": ", ").append(exinfo.getExpression());
        }
        setExpression("[%s]", exlist);
        setType(nextType);
        return this;
    }

    Expression build(ConfigListener configListener, ParserRuleContext ctx) throws Expression.ExpressionException {
        switch (type) {
        case LAMBDA:
        case VARPATH:
        case LITTERAL:
            return new Expression(payload);
        case FORMATTER:
            return new Expression((VarFormatter) payload, configListener.formatters);
        default:
            return new Expression(expression, configListener.groovyClassLoader, configListener.formatters);
        }
    }

    Expression.ExpressionLambda asLambda() {
        switch (type) {
        case LITTERAL:
            return ed -> payload;
        case LAMBDA:
            Expression.ExpressionLambda lambda = getPayload();
            return lambda;
        case VARPATH:
            VariablePath vp = getPayload();
            return ed -> ed.getEvent().getAtPath(vp);
        case FORMATTER:
            VarFormatter vf = getPayload();
            return ed -> vf.format(ed.getEvent());
        case CONSTANT:
            try {
                Object value = new Expression(expression, configListener.groovyClassLoader, configListener.formatters).eval(null);
                return ed -> value;
            } catch (ProcessorException | RuntimeException | Expression.ExpressionException e) {
                return null;
            }
        case OPERATOR:
        case VARIABLE:
        default:
            return null;
        }
    }

}
