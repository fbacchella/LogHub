package loghub.configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.Assert;

import loghub.Helpers.ThrowingFunction;
import loghub.RouteLexer;
import loghub.RouteParser;
import loghub.configuration.ConfigListener.ObjectDescription;
import loghub.configuration.ConfigListener.ObjectWrapped;

public class ConfigurationTools {

    private  ConfigurationTools() {
    }

    public static Object parseFragment(String fragment, Function<RouteParser, ? extends ParserRuleContext> extractor) {
        return ConfigurationTools.parseFragment(CharStreams.fromString(fragment), extractor, new HashMap<>());
    }

    public static Object parseFragment(String fragment, Function<RouteParser, ? extends ParserRuleContext> extractor, Map<String, String> formatters) {
        return ConfigurationTools.parseFragment(CharStreams.fromString(fragment), extractor, formatters);
    }

    public static <T extends ParserRuleContext> Object parseFragment(CharStream fragment, Function<RouteParser, T> extractor, Map<String, String> formatters) {
        RouteLexer lexer = new RouteLexer(fragment);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        RouteParser parser = new RouteParser(tokens);
        parser.removeErrorListeners();
        ConfigErrorListener errListener = new ConfigErrorListener();
        parser.addErrorListener(errListener);

        T tree = extractor.apply(parser);
        ConfigListener conf = new ConfigListener();
        ParseTreeWalker walker = new ParseTreeWalker();
        walker.walk(conf, tree);
        Object o = conf.stack.pop();
        Assert.assertTrue(conf.stack.isEmpty());
        formatters.putAll(conf.formatters);
        return o;
    }

    public static <T> T buildFromFragment(String fragment, Function<RouteParser, ? extends ParserRuleContext> extractor) {
        ObjectDescription parsed = (ObjectDescription) ConfigurationTools.parseFragment(fragment,  extractor);

        ThrowingFunction<Class<T>, T> emptyConstructor = i -> {return i.getConstructor().newInstance();};

        Configuration c = new Configuration();
        return c.parseObjectDescription(parsed, emptyConstructor);
    }

    public static <T> T buildFromFragment(String fragment, Function<RouteParser, ? extends ParserRuleContext> extractor, Map<String, String> formatters) {
        ObjectDescription parsed = (ObjectDescription) ConfigurationTools.parseFragment(fragment,  extractor, formatters);

        ThrowingFunction<Class<T>, T> emptyConstructor = i -> {return i.getConstructor().newInstance();};

        Configuration c = new Configuration();
        return c.parseObjectDescription(parsed, emptyConstructor);
    }

    @SuppressWarnings("unchecked")
    public static <T> T unWrap(String fragment, Function<RouteParser, ? extends ParserRuleContext> extractor) {
        ObjectWrapped parsed = (ObjectWrapped) ConfigurationTools.parseFragment(fragment,  extractor);
        return (T) parsed.wrapped;
    }

}
