package loghub;

import lombok.Data;
import lombok.Getter;

@Data
public class Lambda {
    @Getter
    private final Expression expression;
}
