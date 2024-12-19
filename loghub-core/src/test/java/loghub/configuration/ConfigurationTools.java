package loghub.configuration;

import java.util.function.Function;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.junit.Assert;

import loghub.RouteLexer;
import loghub.RouteParser;
import loghub.configuration.ConfigListener.ObjectWrapped;

public class ConfigurationTools {

    private  ConfigurationTools() {
    }

    public static Object parseFragment(String fragment, Function<RouteParser, ? extends ParserRuleContext> extractor) {
        return ConfigurationTools.parseFragment(CharStreams.fromString(fragment), extractor);
    }

    public static <T extends ParserRuleContext> Object parseFragment(CharStream fragment, Function<RouteParser, T> extractor) {
        RouteLexer lexer = new RouteLexer(fragment);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        RouteParser parser = new RouteParser(tokens);
        parser.filter = new GrammarParserFiltering();
        parser.removeErrorListeners();
        ConfigErrorListener errListener = new ConfigErrorListener();
        parser.addErrorListener(errListener);

        T tree = extractor.apply(parser);
        Assert.assertEquals(tree.getText(), tokens.getText());
        ConfigListener conf = ConfigListener.builder().build();
        conf.startWalk(tree, fragment, parser);
        Object o = null;
        if (!conf.stack.isEmpty()) {
            o = conf.stack.pop();
        }
        Assert.assertTrue(conf.stack.isEmpty());
        return o;
    }

    public static <T> T buildFromFragment(String fragment, Function<RouteParser, ? extends ParserRuleContext> extractor) {
        @SuppressWarnings("unchecked")
        ObjectWrapped<T> parsed = (ObjectWrapped<T>) ConfigurationTools.parseFragment(fragment, extractor);
        return parsed.wrapped;
    }

    public static <T> T unWrap(String fragment, Function<RouteParser, ? extends ParserRuleContext> extractor) {
        @SuppressWarnings("unchecked")
        ObjectWrapped<T> parsed = (ObjectWrapped<T>) ConfigurationTools.parseFragment(fragment, extractor);
        return parsed.wrapped;
    }

}
