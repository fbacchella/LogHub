package loghub;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import loghub.jackson.JacksonBuilder;
import lombok.ToString;

@Parameters(commandNames={"espipeline"})
@ToString
public class EsPipelineConvert {

    @Parameter(description = "YAML files")
    public List<String> files = new ArrayList<>();

    @Parameter(names = {"-p", "--pipeline"}, description = "YAML files")
    public String pipelineName = "main";

    public void process() {
        JacksonBuilder<YAMLMapper> builder = JacksonBuilder.get(YAMLMapper.class);
        ObjectReader reader = builder.getReader();
        for (String pipeline: files) {
            try {
                Map<String, Object> o = reader.readValue(Helpers.fileUri(pipeline).toURL());
                @SuppressWarnings("uncheck")
                List<Map<String, Map<String, Object>>> processors = (List<Map<String, Map<String, Object>>>) o.get("processors");
                System.out.format("pipeline[%s] {%n", pipelineName);
                processPipeline(processors, "    ");
                System.out.println("}");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void processPipeline(List<Map<String, Map<String, Object>>> processors, String prefix) {
        for (Map<String, Map<String, Object>> pi: processors) {
            Map.Entry<String, Map<String, Object>> processor = pi.entrySet().stream().findFirst().get();
            Map<String, Object> params = processor.getValue();
            // Useless with loghub
            params.remove("ignore_failure");
            params.remove("ignore_missing");
            params.remove("ignore_empty_value");

            switch (processor.getKey()) {
            case "script":
                script(params, prefix);
                break;
            case "foreach":
                foreach(params, prefix);
                break;
            case "set":
                set(params, prefix);
                break;
            case "remove":
                remove(params, prefix);
                break;
            case "append":
                append(params, prefix);
                break;
            case "rename":
                System.out.format("%s%s < %s |%n", prefix, resolveField(params.get("target_field")), resolveField((params.get("field"))));
                break;
            case "trim":
            case "lowercase":
            case "uppercase":
                stringOperator(processor.getKey(), params, prefix);
                break;
            case "pipeline":
                pipeline(params, prefix);
                break;
            case "date":
                date(params, prefix);
                break;
            case "convert":
                convert(params, prefix);
                break;
            case "grok":
                grok(params, prefix);
                break;
            case "geoip":
                geoip(params, prefix);
                break;
            case "kv":
                kv(params, prefix);
                break;
            default:
                System.out.println(prefix + "// " + processor);
            }
        }
    }

    private void script(Map<String, Object> params, String prefix) {
        String lang = (String) params.remove("lang");
        String source = (String) params.remove("source");
        System.out.format("%s//Script, lang=%s%n", prefix, lang);
        if (! params.isEmpty()) {
            System.out.format("%s//    %s%n", prefix, params);
        }
        System.out.format("%s/*%n", prefix);
        for (String line: source.split("[\n\r\u0085\u2028\u2029]+")) {
            System.out.format("%s  %s%n", prefix, line);
        }
        System.out.format("%s*/%s%n", prefix, params);
    }

    private void foreach(Map<String, Object> params, String prefix) {
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> process = (Map<String, Map<String, Object>>) params.remove("processor");
        process.get("kv").put("iterate", true);
        if ("_ingest._value".equals(process.get("kv").get("field")) ) {
            process.get("kv").put("field", params.remove("field"));
        }
        params.remove("if");
        System.out.format("%s// foreach%s%n", prefix, params.isEmpty() ? "": params);
        processPipeline(List.of(process), prefix);
    }
    private void set(Map<String, Object> params, String prefix) {
        params.remove("description");
        Object source = params.containsKey("value") ? resolveValue(params.remove("value")) : resolveField(params.remove("copy_from"));
        String field = resolveField((params.remove("field")));
        System.out.format("%s%s = %s |%n", etlFilter(prefix, params), field, source);
    }

    private void stringOperator(String operator, Map<String, Object> params, String prefix) {
        params.remove("description");
        Object destination = params.containsKey("target_field") ? params.remove("target_field") : params.get("field");
        String field = resolveField((params.remove("field")));
        System.out.format("%s%s = %s(%s) |%n", etlFilter(prefix, params), resolveField(destination), operator, field);
    }

    private void remove(Map<String, Object> params, String prefix) {
        params.remove("description");
        if (params.get("field") instanceof String) {
            String field = resolveField((params.remove("field")));
            System.out.format("%s%s- |%n", etlFilter(prefix, params), field);
        } else {
            @SuppressWarnings("uncheck")
            List<String> fields = (List<String>) params.remove("field");
            for (String subfield: fields) {
                System.out.format("%s%s- |%n", etlFilter(prefix, params), resolveField(subfield));
            }
        }
    }

    private void append(Map<String, Object> params, String prefix) {
        params.remove("description");
        Object value = resolveValue(params.remove("value"));
        String field = resolveField((params.remove("field")));
        if (value instanceof List) {
            List<?> values = (List<?>) value;
            for (Object subvalue: values) {
                System.out.format("%s%s =+ %s |%n", etlFilter(prefix, params), field, resolveValue(subvalue));
            }
        } else {
            System.out.format("%s%s =+ %s |%n", etlFilter(prefix, params), field, value);
        }
    }

    private String etlFilter(String prefix, Map<String, Object> params) {
        String ifExpress = (String) params.remove("if");
        if ( !params.isEmpty()) {
            System.out.format("%s// %s", prefix, params);
        }
        if (ifExpress != null && !ifExpress.isBlank()) {
            String transformed = resolveExpression(ifExpress);
            return transformed != null ? String.format("%s%s ? ", prefix, transformed) : String.format("%s// %s ? ", prefix, ifExpress);
        } else {
            return prefix;
        }
    }

    private final Pattern varPattern = Pattern.compile("ctx\\??\\.([_.a-zA-Z0-9?]+)");
    private final Pattern stringPattern = Pattern.compile("'([^']*)'");
    private final Pattern containsPattern = Pattern.compile("(\\[.*]).contains\\((.*)\\)");
    Map<Pattern, Function<MatchResult, String>> transformers = Map.ofEntries(
            Map.entry(Pattern.compile("ctx\\??\\.([_.a-zA-Z0-9?]+)"), mr -> "[" + mr.group(1).replace(".", " ").replace("?", "") + "]"),
            Map.entry(Pattern.compile("'([^']*)'"), mr -> "\"" + mr.group(1) + "\""),
            Map.entry(Pattern.compile("(\\[.*]).contains\\((.*)\\)"), mr -> mr.group(2) + " in " + mr.group(1))
            );
    private String resolveExpression(String expr) {
        for (Map.Entry<Pattern, Function<MatchResult, String>> e: transformers.entrySet()) {
            expr = e.getKey().matcher(expr).replaceAll(e.getValue());
        }
        //expr = varPattern.matcher(expr).replaceAll(this::convert);
        //expr = stringPattern.matcher(expr).replaceAll(mr -> "\"" + mr.group(1) + "\"");
        //expr = containsPattern.matcher(expr).replaceAll(mr -> mr.group(2) + " in " + mr.group(1));
        return expr;
     }

    private String convert(MatchResult mr) {
        return "[" + mr.group(1).replace(".", " ").replace("?", "") + "]";
    }

    Pattern ingestPipelinePattern = Pattern.compile("\\{\\{ IngestPipeline \"(.*)\" }}");
    private void pipeline(Map<String, Object> params, String prefix) {
        String ifexpr = (String) params.get("if");
        if (ifexpr != null && ! ifexpr.isBlank()) {
            System.out.format(prefix + "// %s ?%n", ifexpr);
        }
        String pipelineExpress = (String) params.get("name");
        Matcher m = ingestPipelinePattern.matcher(pipelineExpress);
        if (m.matches()) {
            String pipelineName = m.group(1);
            System.out.format(prefix + "$%s |%n", pipelineName);
        }
    }

    private void date(Map<String, Object> params, String prefix) {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("patterns", params.remove("formats"));
        attributes.put("timezone", resolveValue(params.remove("timezone")));
        doProcessor(prefix, "loghub.processors.DateParser", filterComments(params, attributes), attributes);
    }

    private void grok(Map<String, Object> params, String prefix) {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("patterns", params.remove("patterns"));
        if (params.containsKey("pattern_definitions")) {
            attributes.put("customPatterns", params.remove("pattern_definitions"));
        }
        doProcessor(prefix, "loghub.processors.Grok", filterComments(params, attributes), attributes);
    }

    private void geoip(Map<String, Object> params, String prefix) {
        Map<String, Object> attributes = new HashMap<>();
        Object types = params.remove("properties");
        attributes.put("types", types != null ? types : List.of("country","city", "location"));
        String geoipdb = (String) params.remove("database_file");
        attributes.put("geoipdb", geoipdb != null ? geoipdb : "/usr/share/GeoIP/GeoIP2-City.mmdb");
        attributes.put("refresh", "P2D");
        attributes.put("field", resolveValue(params.remove("field")));
        attributes.put("destination", resolveValue(params.remove("target_field")));
        doProcessor(prefix, "loghub.processors.Geoip2", filterComments(params, attributes), attributes);
    }

    private void kv(Map<String, Object> params, String prefix) {
        Map<String, Object> attributes = new HashMap<>();
        char field_split = Optional.ofNullable(params.remove("field_split")).map(String.class::cast).map(s -> s.charAt(0)).orElse(' ');
        char value_split = Optional.ofNullable(params.remove("value_split")).map(String.class::cast).map(s -> s.charAt(0)).orElse('=');
        Character trim_key = Optional.ofNullable(params.remove("trim_key")).map(String.class::cast).map(s -> s.charAt(0)).orElse(null);
        attributes.put("parser", String.format("\"(?<name>[^%s]+)%s(?<value>[^%s]*)%s\"", value_split, value_split, field_split, (trim_key!= null ? String.format("%s*", trim_key) : "")));
        doProcessor(prefix, "loghub.processors.VarExtractor", filterComments(params, attributes), attributes);
    }

    private void convert(Map<String, Object> params, String prefix) {
        Map<String, Object> attributes = new HashMap<>();
        String className;
        switch (params.remove("type").toString()) {
            case "ip":
                className = "java.net.InetAddress";
                break;
            default:
                className = "java.lang.String";
        }
        attributes.put("className", resolveValue(className));
        doProcessor(prefix, "loghub.processors.Convert", filterComments(params, attributes), attributes);
    }

    private void doProcessor(String prefix, String processor, String comment, Map<String, Object> fields) {
        if (fields.containsKey("description") && fields.get("description") != null && ! fields.get("description").toString().isBlank() ) {
            System.out.format("%s// %s%n", prefix, fields.remove("description"));
        }
        System.out.format("%s%s {%n", prefix, processor);
        if (comment != null && ! comment.isBlank()) {
            System.out.format("%s    // %s%n", prefix, comment);
        }
        for (Map.Entry<String, Object> e: fields.entrySet()) {
            if ("if".equals(e.getKey()) && e.getValue() != null) {
                String transformed = resolveExpression(e.getValue().toString());
                if (transformed != null) {
                    System.out.format("%s    if: %s%n", prefix, transformed);
                } else {
                    System.out.format("%s    //if: %s%n", prefix, e.getValue());
                }
            } else if ("failure".equals(e.getKey()) && e.getValue() != null) {
                System.out.format("%s    failure: (%n", prefix);
                processPipeline((List<Map<String, Map<String, Object>>>) e.getValue(), prefix + "        ");
                System.out.format("%s    ),%n", prefix);
            } else if ("iterate".equals(e.getKey()) && Boolean.TRUE.equals(e.getValue())) {
                System.out.format("%s    iterate: true%n", prefix);
            } else if ("iterate".equals(e.getKey()) && Boolean.FALSE.equals(e.getValue())) {
                System.out.format("%s    iterate: false%n", prefix);
            } else if (e.getValue() instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) e.getValue();
                StringBuffer definitions = new StringBuffer();
                for (Map.Entry<String, Object> me: map.entrySet()) {
                    definitions.append(prefix).append("        \"").append(me.getKey()).append("\": \"").append(me.getValue()).append("\",\n");
                }
                System.out.format("%s    %s: {%n%s    %s},%n", prefix, e.getKey(), definitions, prefix);
            } else if (e.getValue() instanceof List) {
                List<?> val = (List<?>) e.getValue();
                String valStr = val.stream()
                                   .map(i -> String.format("\"%s\"", i))
                                   .collect(Collectors.joining(", "));
                System.out.format("%s    %s: [%s],%n", prefix, e.getKey(), valStr);
            } else if (e.getValue() != null) {
                System.out.format("%s    %s: %s,%n", prefix, e.getKey(), e.getValue());
            }
        }
        System.out.format("%s} |%n", prefix);
    }

    private static final Pattern valuePattern = Pattern.compile("\\{\\{\\{(.*)}}}");

    private Object resolveValue(Object value) {
        if (value instanceof String) {
            Matcher m = valuePattern.matcher((String)value);
            if (m.matches()) {
                return resolveField(m.group(1));
            } else {
                return String.format("\"%s\"", value);
            }
        } else {
            return value;
        }
    }

    private String resolveField(Object name) {
        if ("_ingest.on_failure_message".equals(name)) {
            return "[@lastException]";
        } else if (name != null) {
            List<String> path = new ArrayList<>();
            for (String parts: name.toString().split("\\.")) {
                char[] letters = parts.toCharArray();
                boolean identifier = Character.isJavaIdentifierStart(letters[0]);
                for (int i = 1; i < letters.length; i++) {
                    identifier &= Character.isJavaIdentifierPart(letters[i]);
                }
                path.add(identifier ? parts : String.format("\"%s\"", parts));
            }
            return "[" + String.join(" ", path) + "]";
        } else {
            return null;
        }
    }

    private String filterComments(Map<String, Object> params, Map<String, Object> attributes) {
        attributes.put("field", resolveField(params.remove("field")));
        attributes.put("destination", resolveField(params.remove("target_field")));
        attributes.put("if", params.remove("if"));
        attributes.put("failure", params.remove("on_failure"));
        attributes.put("iterate", params.remove("iterate"));
        attributes.put("description", params.remove("description"));
        return params.isEmpty() ? null : params.toString();
    }

}
