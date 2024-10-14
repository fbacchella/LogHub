package loghub.commands;

import java.io.IOException;
import java.io.UncheckedIOException;
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

import loghub.Helpers;
import loghub.jackson.JacksonBuilder;
import lombok.ToString;

@Parameters(commandNames={"espipeline"})
@ToString
public class EsPipelineConvert implements BaseCommand {

    @Parameter(names = {"-p", "--pipeline"}, description = "YAML files")
    public String pipelineName = "main";

    @Parameter(names = {"--help", "-h"}, help = true)
    private boolean help;

    public int run(List<String> unknownOptions) {
        JacksonBuilder<YAMLMapper> builder = JacksonBuilder.get(YAMLMapper.class);
        ObjectReader reader = builder.getReader();
        for (String pipeline: unknownOptions) {
            try {
                Map<String, Object> o = reader.readValue(Helpers.fileUri(pipeline).toURL());
                @SuppressWarnings("unchecked")
                List<Map<String, Map<String, Object>>> processors = (List<Map<String, Map<String, Object>>>) o.get("processors");
                System.out.format("pipeline[%s] {%n", pipelineName);
                processPipeline(processors, "    ");
                System.out.println("}");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return ExitCode.OK;
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
                rename(params, prefix);
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
            case "split":
                split(params, prefix);
                break;
            case "kv":
                kv(params, prefix);
                break;
            case "dissect":
                dissect(params, prefix);
                break;
            case "gsub":
                gsub(params, prefix);
                break;
            case "json":
                json(params, prefix);
                break;
            case "user_agent":
                userAgent(params, prefix);
                break;
            default:
                System.out.println(prefix + "// " + processor);
            }
        }
    }

    private void rename(Map<String, Object> params, String prefix) {
        String target_field = (String) params.remove("target_field");
        String field = (String) params.remove("field");
        System.out.format("%s%s < %s |%n", etlFilter(prefix, params), resolveField(target_field), resolveField(field));
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
        System.out.format("%s*/%n", prefix);
    }

    private void foreach(Map<String, Object> params, String prefix) {
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> process = (Map<String, Map<String, Object>>) params.remove("processor");
        if (process.containsKey("kv")) {
            Map<String, Object> kv = process.get("kv");
            kv.put("iterate", true);
            if ("_ingest._value".equals(kv.get("field")) ) {
                kv.put("field", params.remove("field"));
            }
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
            @SuppressWarnings("unchecked")
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

    private static final List<Map.Entry<Pattern, Function<MatchResult, String>>> transformers = List.of(
            Map.entry(Pattern.compile("((?<!\\\\))\n"), mr -> mr.group(1)),
            Map.entry(Pattern.compile("ctx\\??\\.([_.a-zA-Z0-9?]+)"), mr -> "[" + mr.group(1).replace(".", " ").replace("?", "") + "]"),
            Map.entry(Pattern.compile("'([^']*)'"), mr -> "\"" + mr.group(1) + "\""),
            Map.entry(Pattern.compile("(\\[.*]).contains\\((.*)\\)"), mr -> mr.group(2) + " in list" + mr.group(1).replace("[", "(").replace("]", ")"))
    );

    private String resolveExpression(String expr) {
        String newExpression = expr;
        for (Map.Entry<Pattern, Function<MatchResult, String>> e: transformers) {
            newExpression = e.getKey().matcher(newExpression).replaceAll(e.getValue());
        }
        return newExpression;
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
        attributes.put("geoipdb", resolveValue(geoipdb != null ? geoipdb : "/usr/share/GeoIP/GeoIP2-City.mmdb"));
        attributes.put("refresh", resolveValue("P2D"));
        attributes.put("field", resolveField(params.remove("field")));
        attributes.put("destination", resolveField(params.remove("target_field")));
        doProcessor(prefix, "loghub.processors.Geoip2", filterComments(params, attributes), attributes);
    }

    private void split(Map<String, Object> params, String prefix) {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("pattern", resolveValue(params.remove("separator")));
        doProcessor(prefix, "loghub.processors.Split", filterComments(params, attributes), attributes);
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
        String type = (String) params.remove("type");
        switch (type) {
        case "ip":
            className = "java.net.InetAddress";
            break;
        case "long":
            className = "java.lang.Long";
            break;
        case "integer":
            className = "java.lang.Integer";
            break;
        case "string":
            className = "java.lang.String";
            break;
        default:
            throw new UnsupportedOperationException(type);
        }
        attributes.put("className", resolveValue(className));
        doProcessor(prefix, "loghub.processors.Convert", filterComments(params, attributes), attributes);
    }

    private void dissect(Map<String, Object> params, String prefix) {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("pattern", resolveValue(params.remove("pattern")));
        doProcessor(prefix, "loghub.processors.Dissect", filterComments(params, attributes), attributes);
    }

    private void gsub(Map<String, Object> params, String prefix) {
        Object field = resolveField(params.remove("field"));
        Object target_field = resolveField(params.remove("target_field"));
        if (target_field == null) {
            target_field = field;
        }
        Object pattern = params.remove("pattern");
        Object replacement = params.remove("replacement");
        System.out.format("%s%s = gsub(%s, /%s/, %s) |%n", etlFilter(prefix, params), target_field, field, pattern, resolveValue(replacement));
    }

    private void json(Map<String, Object> params, String prefix) {
        Map<String, Object> attributes = new HashMap<>();
        String field = resolveField(params.remove("field"));
        String target_field = resolveField(params.remove("target_field"));
        if (target_field != null) {
            attributes.put("field", field.replace("[", "[. "));
            System.out.format("%spath%s(%n", prefix, target_field);
            doProcessor(prefix + "    ", "loghub.processors.ParseJson", filterComments(params, attributes), attributes);
            System.out.format("%s) |%n", prefix);
        } else {
            attributes.put("field", field);
            doProcessor(prefix, "loghub.processors.ParseJson", filterComments(params, attributes), attributes);
        }
    }

    private void userAgent(Map<String, Object> params, String prefix) {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("field", resolveField(params.remove("field")));
        attributes.put("destination", resolveField(Optional.ofNullable(params.remove("target_field")).orElse("user_agent")));
        doProcessor(prefix, "loghub.processors.UserAgent", filterComments(params, attributes), attributes);
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
                    definitions.append(prefix).append("        \"").append(me.getKey()).append("\": ").append(resolveValue(me.getValue())).append(",\n");
                }
                System.out.format("%s    %s: {%n%s    %s},%n", prefix, e.getKey(), definitions, prefix);
            } else if (e.getValue() instanceof List) {
                List<?> val = (List<?>) e.getValue();
                String valStr = val.stream()
                                        .map(this::resolveValue)
                                        .map(String.class::cast)
                                        .collect(Collectors.joining(", "));
                System.out.format("%s    %s: [%s],%n", prefix, e.getKey(), valStr);
            } else if (e.getValue() != null) {
                System.out.format("%s    %s: %s,%n", prefix, e.getKey(), e.getValue());
            }
        }
        System.out.format("%s} |%n", prefix);
    }

    private static final Pattern valuePattern = Pattern.compile("\\{\\{(.*)}}");

    private Object resolveValue(Object value) {
        if (value instanceof String) {
            Matcher m = valuePattern.matcher((String)value);
            if (m.matches()) {
                String variable  = m.group(1);
                if (variable.startsWith("{") && variable.endsWith("}")) {
                    variable = variable.substring(1, variable.length() -1);
                }
                return resolveField(variable.trim());
            } else {
                String valStr = (String) value;
                return String.format("\"%s\"", valStr.replace("\\", "\\\\").replace("\"", "\\\""));
            }
        } else if (value == null) {
            return "null";
        } else {
            return value.toString();
        }
    }

    private String resolveField(Object name) {
        if ("_ingest.on_failure_message".equals(name)) {
            return "[@lastException]";
        } else if ("_ingest.timestamp".equals(name)) {
            return "now";
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
        Optional.ofNullable(params.remove("field")).map(this::resolveField).ifPresent(v -> attributes.put("field", v));
        Optional.ofNullable(params.remove("target_field")).map(this::resolveField).ifPresent(v -> attributes.put("destination", v));
        Optional.ofNullable(params.remove("if")).ifPresent(v -> attributes.put("if", v));
        Optional.ofNullable(params.remove("on_failure")).ifPresent(v -> attributes.put("failure", v));
        Optional.ofNullable(params.remove("iterate")).ifPresent(v -> attributes.put("iterate", v));
        Optional.ofNullable(params.remove("description")).ifPresent(v -> attributes.put("description", v));
        return params.isEmpty() ? null : params.toString();
    }

    @Override
    public <T> Optional<T> getField(String name, Class<T> tClass) {
        if ("help".equals(name)) {
            return Optional.of((T) Boolean.valueOf(help));
        } else {
            return Optional.empty();
        }
    }
}
