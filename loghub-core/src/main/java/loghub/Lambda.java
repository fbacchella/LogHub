package loghub;

public record Lambda(Expression expression) {
    @Deprecated
    public Expression getExpression() {
        return expression();
    }
}
