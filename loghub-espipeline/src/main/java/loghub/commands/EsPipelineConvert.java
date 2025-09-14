package loghub.commands;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Consumer;
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
import loghub.VariablePath;
import loghub.jackson.JacksonBuilder;
import lombok.ToString;

@Parameters(commandNames={"espipeline"})
@ToString
public class EsPipelineConvert implements BaseParametersRunner {

    private static class PipelineOutput implements Closeable {
        private final StringBuilder buffer = new StringBuilder();
        private String prefix;
        Deque<Integer> stack = new ArrayDeque<>();
        PipelineOutput(String pipelineName) {
            buffer.append("pipeline[%s] {%n".formatted(pipelineName));
            stack.push(-1);
            prefix = "    ";
        }
        private void removePipeSymbol() {
            int pos = stack.getLast();
            // Remove the last " |"
            if (! buffer.isEmpty() && pos != -1) {
                buffer.deleteCharAt(pos);
                buffer.deleteCharAt(pos - 1);
            }
        }
        private void increasePrefix() {
            prefix = prefix + "    ";
        }
        private void reducePrefix() {
            if (prefix.length() >= 4) {
                prefix = prefix.substring(4);
            }
        }
        private void formatBuffer(String pattern, Object... args) {
            buffer.append(pattern.formatted(args));
        }
        private void appendPipeSymbol() {
            stack.removeLast();
            stack.addLast(buffer.length() + 1);
            buffer.append(" |\n");
        }
        void startPipeline(String wrapper) {
            formatBuffer("%s%s (%n", prefix, wrapper);
            stack.addLast(-1);
            increasePrefix();
        }
        void endPipeline() {
            removePipeSymbol();
            reducePrefix();
            formatBuffer("%s)%n", prefix);
        }
        void startFieldPipeline(String field) {
            formatBuffer("%s%s: (%n", prefix, field);
            stack.addLast(-1);
            increasePrefix();
        }
        void endFieldPipeline() {
            removePipeSymbol();
            reducePrefix();
            formatBuffer("%s),%n", prefix);
        }
        void startProcessor(String pclazz) {
            buffer.append("%s%s {%n".formatted(prefix, pclazz));
            increasePrefix();
        }
        void endProcessor() {
            reducePrefix();
            buffer.append("%s}%n".formatted(prefix));
        }
        void endStep() {
            if (!buffer.isEmpty()) {
                buffer.deleteCharAt(buffer.length() - 1);
            }
            appendPipeSymbol();
        }
        void startPath(String varPath) {
            stack.addLast(-1);
            formatBuffer("%spath%s (%n", prefix, varPath);
            increasePrefix();
        }
        void endPath() {
            removePipeSymbol();
            stack.removeLast();
            reducePrefix();
            buffer.append(prefix).append(")");
            appendPipeSymbol();
         }
        void format(String pattern, Object... args) {
            buffer.append(prefix);
            buffer.append(pattern.formatted(args));
            buffer.append("\n");
        }
        void comment(String comment) {
            format("// %s", comment);
        }
        void flush(Writer stream) throws IOException {
            removePipeSymbol();
            stream.write(buffer.toString());
            stream.write("}\n");
        }
        String appendPrefix(String customPrefix) {
            try {
                return prefix;
            } finally {
                prefix = prefix + customPrefix;
            }
        }
        void setPrefix(String customPrefix) {
            prefix = customPrefix;
        }
        public void close() {
            buffer.delete(0, buffer.length());
        }
    }

    @Parameter(names = {"-o", "--output"}, description = "The output file")
    public String outputConfiguration = null;

    private PipelineOutput output;
    private final ObjectReader reader;
    private final Map<String, Consumer<Map<String, Object>>> keywords;

    public EsPipelineConvert() {
        JacksonBuilder<YAMLMapper> builder = JacksonBuilder.get(YAMLMapper.class);
        reader = builder.getReader();
        keywords = Map.ofEntries(
                Map.entry("script", this::script),
                Map.entry("set", p -> ifWrapper(p, this::set)),
                Map.entry("remove", p -> ifWrapper(p, this::remove)),
                Map.entry("foreach", this::foreach),
                Map.entry("append", p -> ifWrapper(p, this::append)),
                Map.entry("rename", p -> ifWrapper(p, this::rename)),
                Map.entry("trim", p -> ifWrapper(p, pr -> stringOperator("trim", pr))),
                Map.entry("lowercase", p -> ifWrapper(p, pr -> stringOperator("lowercase", pr))),
                Map.entry("uppercase", p -> ifWrapper(p, pr -> stringOperator("uppercase", pr))),
                Map.entry("gsub", p -> ifWrapper(p, this::gsub)),
                Map.entry("pipeline", p -> ifWrapper(p, this::pipeline)),
                Map.entry("date", p -> doSimpleProcessor("loghub.processors.DateParser", p, "formats", "patterns", "timezone", "timezone")),
                Map.entry("convert", this::convert),
                Map.entry("grok", this::grok),
                Map.entry("geoip", this::geoip),
                Map.entry("split", p -> doSimpleProcessor("loghub.processors.Dissect", p, "pattern", "separator")),
                Map.entry("kv", this::kv),
                Map.entry("dissect", p -> doSimpleProcessor("loghub.processors.Dissect", p, "pattern", "pattern")),
                Map.entry("json", this::json),
                Map.entry("user_agent", p -> doSimpleProcessor("loghub.processors.UserAgent", p)),
                Map.entry("urldecode", p -> doSimpleProcessor("loghub.processors.DecodeUrl", p)),
                Map.entry("csv", this::csv)
        );
    }

    @Override
    public void reset() {
        outputConfiguration = null;
    }

    public int run(List<String> mainParameters, PrintWriter out, PrintWriter err) {
        Pattern spliter = Pattern.compile("@");
        try (Writer w = getWriter(out)){
            for (String pipeline : mainParameters) {
                try {
                    String pipelineName = "default";
                    String[] informations = spliter.split(pipeline);
                    if (informations.length == 2) {
                        pipelineName = informations[0];
                        pipeline = informations[1];
                    }
                    Reader r = new InputStreamReader(Helpers.fileUri(pipeline).toURL().openStream());
                    runParse(r, w, pipelineName);
                } catch (IOException e) {
                    err.format("Failed to read %s, error: %s%n", pipeline, Helpers.resolveThrowableException(e));
                }
            }
            return ExitCode.OK;
        } catch (IOException e) {
            err.format(
                    "Failed to save output %s, error: %s%n",
                    outputConfiguration != null ? outputConfiguration : "stdout",
                    Helpers.resolveThrowableException(e)
            );
            return ExitCode.INVALIDARGUMENTS;
        }
    }

    private Writer getWriter(Writer out) throws IOException {
        if (outputConfiguration != null) {
            return Files.newBufferedWriter(Path.of(outputConfiguration));
        } else {
            return out;
        }
    }

    void runParse(Reader r, Writer w, String pipelineName) throws IOException {
        Map<String, Object> yamlContent = reader.readValue(r);
        try (PipelineOutput o = new PipelineOutput(pipelineName)) {
            output = o;
            @SuppressWarnings("unchecked")
            List<Map<String, Map<String, Object>>> processors = (List<Map<String, Map<String, Object>>>) yamlContent.get("processors");
            processPipeline(processors);
            output.flush(w);
        }
    }

    private void processPipeline(List<Map<String, Map<String, Object>>> processors) {
        for (Map<String, Map<String, Object>> pi: processors) {
            // Will only iterate once, each processor is a single entry map
            for (Map.Entry<String, Map<String, Object>> processor: pi.entrySet()) {
                Map<String, Object> params = processor.getValue();
                // Useless with loghub
                params.remove("ignore_failure");
                params.remove("ignore_missing");
                params.remove("ignore_empty_value");
                params.remove("tag");
                String processorName = processor.getKey();
                if (keywords.containsKey(processorName)) {
                    keywords.get(processorName).accept(params);
                } else {
                    output.comment(processor.toString());
                }
            }
        }
    }

    private void rename(Map<String, Object> params) {
        String targetField = (String) params.remove("target_field");
        String field = (String) params.remove("field");
        output.format("%s < %s", resolveField(targetField), resolveField(field));
        output.endStep();
    }

    private void script(Map<String, Object> params) {
        String lang = (String) params.remove("lang");
        String source = (String) params.remove("source");
        if (lang != null) {
            output.comment("Script, lang=%s".formatted(lang));
        } else {
            output.comment("Script");
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> scriptParams = (Map<String, Object>) params.remove("params");
        if (scriptParams != null && ! scriptParams.isEmpty()) {
            output.comment("  Params");
            scriptParams.forEach((s, o) -> output.comment("    %s: %s".formatted(s, o)));
        }
        params.forEach((s, o) -> output.comment("  %s: %s".formatted(s, o)));
        output.format("/*");
        for (String line: source.split("[\n\r\u0085\u2028\u2029]+")) {
            output.format("  %s", line);
        }
        output.format("*/");
    }

    private void foreach(Map<String, Object> params) {
        String test;
        if (params.containsKey("if")) {
            test = etlFilter(new HashMap<>(Map.of("if", params.remove("if")))) + " ";
        } else {
            test = "";
        }
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> process = (Map<String, Map<String, Object>>) params.remove("processor");
        output.startPipeline("%sforeach%s".formatted(test, resolveField(params.remove("field"))));
        processPipeline(List.of(process));
        output.endPipeline();
        output.endStep();
    }

    private void set(Map<String, Object> params) {
        Object source = params.containsKey("value") ? resolveValue(params.remove("value")) : resolveField(params.remove("copy_from"));
        String field = resolveField((params.remove("field")));
        output.format("%s = %s", field, source);
        output.endStep();
    }

    private void stringOperator(String operator, Map<String, Object> params) {
        Object destination = params.containsKey("target_field") ? params.remove("target_field") : params.get("field");
        String field = resolveField((params.remove("field")));
        output.format("%s = %s(%s)", resolveField(destination), operator, field);
        output.endStep();
    }

    private void remove(Map<String, Object> params) {
        if (params.get("field") instanceof String) {
            String field = resolveField((params.remove("field")));
            output.format("%s-", field);
            output.endStep();
        } else {
            @SuppressWarnings("unchecked")
            List<String> fields = (List<String>) params.remove("field");
            for (String subfield: fields) {
                output.format("%s-", resolveField(subfield));
                output.endStep();
            }
        }
    }

    private void append(Map<String, Object> params) {
        Object value = resolveValue(params.remove("value"));
        String field = resolveField((params.remove("field")));
        if (value instanceof List<?> values) {
            for (Object subvalue: values) {
                output.format("%s =+ %s", field, resolveValue(subvalue));
                output.endStep();
            }
        } else {
            output.format("%s =+ %s", field, value);
            output.endStep();
        }
    }

    private String etlFilter(Map<String, Object> params) {
        String ifExpress = (String) params.remove("if");
        if ( !params.isEmpty()) {
            output.comment(params.toString());
        }
        if (ifExpress != null && !ifExpress.isBlank()) {
            String transformed = resolveExpression(ifExpress);
            return transformed != null ? String.format("%s ?", transformed) : String.format("// %s ? ", ifExpress);
        } else {
            return "";
        }
    }

    private void ifWrapper(Map<String, Object> params, Consumer<Map<String, Object>> method) {
        if (params.containsKey("if")) {
            Object fields = params.get("field");
            boolean multiple = fields instanceof List && ((List<?>) fields).size() > 1;
            String test = etlFilter(new HashMap<>(Map.of("if", params.remove("if"))));
            if (multiple) {
                output.startPipeline(test);
                // the if has been removed
                method.accept(params);
                output.endPipeline();
                output.endStep();
            } else {
                String previousPrefix = output.appendPrefix(test + " ");
                method.accept(params);
                output.setPrefix(previousPrefix);

            }
        } else {
            method.accept(params);
         }
    }

    private static final List<Entry<Pattern, Function<MatchResult, String>>> transformers = List.of(
            Map.entry(Pattern.compile("(\\[.*]).contains\\((.*)\\)"), mr -> mr.group(2) + " in list" + mr.group(1).replace("[", "(").replace("]", ")")),
            Map.entry(Pattern.compile("([_.a-zA-Z0-9?]+).contains\\((.*)\\)"), mr -> mr.group(2) + " in " + mr.group(1)),
            Map.entry(Pattern.compile("== null"), mr -> "!= *"),
            Map.entry(Pattern.compile("!= null"), mr -> "== *"),
            Map.entry(Pattern.compile("instanceof ([_.a-zA-Z0-9?]+)"), mr -> mapInstanceof(mr.group(1))),
            Map.entry(Pattern.compile("((?<!\\\\))\n"), mr -> mr.group(1)),
            Map.entry(Pattern.compile("ctx\\??\\.([_.a-zA-Z0-9?]+)"), mr -> "[" + mr.group(1).replace(".", " ").replace("?", "") + "]"),
            Map.entry(Pattern.compile("'([^']*)'"), mr -> "\"" + mr.group(1) + "\"")
    );

    private static String mapInstanceof(String type) {
        return switch (type) {
            case "String" -> "instanceof java.lang.String";
            case "List" -> "instanceof java.util.List";
            default -> "instanceof " + type;
        };
    }

    private String resolveExpression(String expr) {
        String newExpression = expr;
        for (Entry<Pattern, Function<MatchResult, String>> e: transformers) {
            newExpression = e.getKey().matcher(newExpression).replaceAll(e.getValue());
        }
        return newExpression;
    }

    private static final Pattern ingestPipelinePattern = Pattern.compile("\\{\\{ IngestPipeline \"(.*)\" }}");
    private void pipeline(Map<String, Object> params) {
        String pipelineExpress = (String) params.get("name");
        Matcher m = ingestPipelinePattern.matcher(pipelineExpress);
        if (m.matches()) {
            output.format("$%s", m.group(1));
            output.endStep();
        } else {
            output.comment("pipeline %s".formatted(params));
        }
    }

    private void grok(Map<String, Object> params) {
        Map<String, Object> attributes = defaultAttributes(params);
        Optional.ofNullable(params.remove("patterns")).ifPresent(o -> attributes.put("patterns", o));
        Optional.ofNullable(params.remove("pattern_definitions")).ifPresent(o -> attributes.put("customPatterns", o));
        doProcessor("loghub.processors.Grok", filterComments(params, attributes), attributes);
    }

    private void geoip(Map<String, Object> params) {
        Map<String, Object> attributes = defaultAttributes(params);
        Object types = params.remove("properties");
        attributes.put("types", resolveValue(types != null ? types : List.of("country","city", "location")));
        String geoipdb = (String) params.remove("database_file");
        attributes.put("geoipdb", resolveValue(geoipdb != null ? geoipdb : "/usr/share/GeoIP/GeoIP2-City.mmdb"));
        attributes.put("refresh", resolveValue("P2D"));
        doProcessor("loghub.processors.Geoip2", filterComments(params, attributes), attributes);
    }

    private void kv(Map<String, Object> params) {
        Map<String, Object> attributes = defaultAttributes(params);
        char fieldSplit = Optional.ofNullable(params.remove("field_split")).map(String.class::cast).map(s -> s.charAt(0)).orElse(' ');
        char valueSplit = Optional.ofNullable(params.remove("value_split")).map(String.class::cast).map(s -> s.charAt(0)).orElse('=');
        Character trimSey = Optional.ofNullable(params.remove("trim_key")).map(String.class::cast).map(s -> s.charAt(0)).orElse(null);
        attributes.put("parser", String.format("\"(?<name>[^%s]+)%s(?<value>[^%s]*)%s\"", valueSplit, valueSplit, fieldSplit, (trimSey!= null ? String.format("%s*", trimSey) : "")));
        doProcessor("loghub.processors.VarExtractor", filterComments(params, attributes), attributes);
    }

    private void convert(Map<String, Object> params) {
        Map<String, Object> attributes = defaultAttributes(params);
        String className;
        String type = (String) params.remove("type");
        className = switch (type) {
        case "ip" -> "java.net.InetAddress";
        case "long" -> "java.lang.Long";
        case "double" -> "java.lang.Double";
        case "integer" -> "java.lang.Integer";
        case "boolean" -> "java.lang.Boolean";
        case "string" -> "java.lang.String";
        default ->
            throw new UnsupportedOperationException(type);
        };
        if (params.isEmpty()) {
            attributes.put("className", className);
            ifWrapper(attributes, this::simpleConvert);
        } else {
            attributes.put("className", resolveValue(className));
            doProcessor("loghub.processors.Convert", filterComments(params, attributes), attributes);
        }
    }

    private void simpleConvert(Map<String, Object> params) {
        String field = (String) params.remove("field");
        String className = (String) params.remove("className");
        String destination = (String) params.remove("destination");
        output.format("(%s)%s", className, field);
        output.endStep();
        if (destination != null && ! destination.equals(field)) {
            output.format("%s < %s", destination, field);
            output.endStep();
        }
    }

    private void gsub(Map<String, Object> params) {
        Object field = resolveField(params.remove("field"));
        Object targetField = resolveField(params.remove("target_field"));
        if (targetField == null) {
            targetField = field;
        }
        Object pattern = params.remove("pattern");
        Object replacement = params.remove("replacement");
        output.format("%s = gsub(%s, /%s/, %s)", targetField, field, pattern, resolveValue(replacement));
        output.endStep();
    }

    private void json(Map<String, Object> params) {
        if (params.containsKey("target_field")) {
            Map<String, Object> attributes = new LinkedHashMap<>();
            String field = resolveField(params.remove("field"));
            attributes.put("field", field.replace("[", "[. "));
            String target_field = resolveField(params.remove("target_field"));
            output.startPath(target_field);
            doProcessor("loghub.processors.ParseJson", filterComments(params, attributes), attributes);
            output.endPath();
        } else {
            Map<String, Object> attributes = defaultAttributes(params);
            doProcessor("loghub.processors.ParseJson", filterComments(params, attributes), attributes);
        }
    }

    private void csv(Map<String, Object> params) {
        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put("field", resolveField(params.remove("field")));
        @SuppressWarnings("unchecked")
        List<String> target_fields = (List<String>) params.remove("target_fields");
        List<VariablePath> fields = target_fields.stream().map(VariablePath::parse).toList();
        attributes.put("headers", fields);
        doProcessor("loghub.processors.ParseCsv", filterComments(params, attributes), attributes);
    }

    private void doSimpleProcessor(String processor, Map<String, Object> params) {
        doSimpleProcessor(processor, params, Map.of());
    }

    private void doSimpleProcessor(String processor, Map<String, Object> params, Map<String, String> additionnalValues) {
        Map<String, Object> attributes = defaultAttributes(params, additionnalValues);
        doProcessor(processor, filterComments(params, attributes), attributes);
    }

    private void doSimpleProcessor(String processor, Map<String, Object> params, String... additionnalValues) {
        Map<String, String> additionnalValuesMap = LinkedHashMap.newLinkedHashMap(additionnalValues.length / 2);
        for (int i = 0; i < additionnalValues.length; i +=2) {
            additionnalValuesMap.put(additionnalValues[i], additionnalValues[i + 1]);
        }
        Map<String, Object> attributes = defaultAttributes(params, additionnalValuesMap);
        doProcessor(processor, filterComments(params, attributes), attributes);
    }

    private void doProcessor(String processor, String comment, Map<String, Object> fields) {
        if (fields.containsKey("description") && fields.get("description") != null && ! fields.get("description").toString().isBlank() ) {
            output.comment(fields.remove("description").toString());
        }
        output.startProcessor(processor);
        if (comment != null && ! comment.isBlank()) {
            output.comment(comment);
        }
        for (Entry<String, Object> e: fields.entrySet()) {
            if ("if".equals(e.getKey()) && e.getValue() != null) {
                String transformed = resolveExpression(e.getValue().toString());
                if (transformed != null) {
                    output.format("if: %s,", transformed);
                } else {
                    output.comment("if: %s".formatted(e.getValue()));
                }
            } else if ("failure".equals(e.getKey()) && e.getValue() != null) {
                output.startFieldPipeline("failure");
                processPipeline((List<Map<String, Map<String, Object>>>) e.getValue());
                output.endFieldPipeline();
            } else if ("iterate".equals(e.getKey()) && Boolean.TRUE.equals(e.getValue())) {
                output.format("iterate: true");
            } else if ("iterate".equals(e.getKey()) && Boolean.FALSE.equals(e.getValue())) {
                output.format(" iterate: false");
            } else if (e.getValue() instanceof Map) {
                output.format("%s: {", e.getKey());
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) e.getValue();
                for (Entry<String, Object> me: map.entrySet()) {
                    output.format("    \"%s\": %s,", me.getKey(), resolveValue(me.getValue()));
                }
                output.format("},");
            } else if (e.getValue() instanceof List<?> val) {
                String valStr = val.stream()
                                        .map(String.class::cast)
                                        .collect(Collectors.joining(", "));
                output.format("%s: [%s],", e.getKey(), valStr);
            } else if (e.getValue() != null) {
                output.format("%s: %s,", e.getKey(), e.getValue());
            }
        }
        output.endProcessor();
        output.endStep();
    }

    private static final Pattern valuePattern = Pattern.compile("\\{\\{(.*)}}");

    private Object resolveValue(Object value) {
        if (value instanceof List<?> list) {
            return list.stream().map(this::resolveValue).toList();
        } else if (value instanceof String valueString) {
            Matcher m = valuePattern.matcher(valueString);
            if (m.matches()) {
                String variable  = m.group(1);
                if (variable.startsWith("{") && variable.endsWith("}")) {
                    variable = variable.substring(1, variable.length() -1);
                }
                return resolveField(variable.trim());
            } else {
                return String.format("\"%s\"", valueString.replace("\\", "\\\\").replace("\"", "\\\""));
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
        } else if ("@timestamp".equals(name)) {
            return "[@timestamp]";
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

    private Map<String, Object> defaultAttributes(Map<String, Object> params) {
        return defaultAttributes(params, Map.of());
    }

    private Map<String, Object> defaultAttributes(Map<String, Object> params, Map<String, String> additionnalValues) {
        Map<String, Object> attributes = new LinkedHashMap<>();
        Optional.ofNullable(params.remove("field")).map(this::resolveField).ifPresent(o -> attributes.put("field", o));
        Optional.ofNullable(params.remove("target_field")).map(this::resolveField).ifPresent(o -> attributes.put("destination", o));
        for (Map.Entry<String, String> s: additionnalValues.entrySet()) {
            Optional.ofNullable(params.remove(s.getKey())).map(this::resolveValue).ifPresent(o -> attributes.put(s.getValue(), o));
        }
        return attributes;
    }

}
