package loghub;

public record Lambda(Expression expression) {
    public Expression getExpression() {
        return expression();
    }
}
