package loghub.commands;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.jackson.JacksonBuilder;

public class TestEsPipelineConvert {

    private Properties convert(String esPipeline, String expected) throws IOException {
        JacksonBuilder<YAMLMapper> builder = JacksonBuilder.get(YAMLMapper.class);
        ObjectReader reader = builder.getReader();
        Object content = reader.readValue(esPipeline);
        if (content instanceof List<?>) {
            content = Map.of("processors", content);
            esPipeline = builder.getWriter().writeValueAsString(content);
        }
        Reader r = new StringReader(esPipeline);
        CharArrayWriter w = new CharArrayWriter();
        EsPipelineConvert epc = new EsPipelineConvert();
        epc.runParse(r, w, "apipeline");
        String converted = new String(w.toCharArray());
        Properties props = Configuration.parse(new StringReader(converted));
        Assert.assertEquals(expected, converted);
        return props;
    }

    @Test
    public void testPipeline() throws IOException {
        String esPipeline = """
          - pipeline:
              name: '{{ IngestPipeline "pipe1" }}'
          - pipeline:
              name: '{{ IngestPipeline "pipe2" }}'
              if: ctx.json?.cltintip != ''
        """;

        String expected = """
        pipeline[apipeline] {
            $pipe1 |
            [json cltintip] != "" ? $pipe2
        }
        """;
        convert(esPipeline, expected);
    }

    @Test
    public void testRemove() throws IOException {
        String esPipeline = """
        - remove:
            field:
              - a
              - b
              - c
            ignore_missing: true
            if: ctx.a instanceof String
            tag: tag1
            description: descr
        - remove:
            field: d
        """;

        String expected = """
        pipeline[apipeline] {
            [a] instanceof java.lang.String ? (
                [a]- |
                [b]- |
                [c]-
            ) |
            [d]-
        }
        """;
        convert(esPipeline, expected);
    }

    @Test
    public void testSet() throws IOException {
        String esPipeline = """
          - set:
              field: ecs.version
              tag: set_ecs_version
              value: 8.11.0
          - set:
              field: event.kind
              value: pipeline_error
              tag: set_pipeline_error_into_event_kind
              if: ctx.error?.message != null
        """;

        String expected = """
        pipeline[apipeline] {
            [ecs version] = "8.11.0" |
            [error message] == * ? [event kind] = "pipeline_error"
        }
        """;
        convert(esPipeline, expected);
    }

    @Test
    public void testDissect() throws IOException {
        String esPipeline = """
        - dissect:
              field: user.email
              pattern: '%{user.name}@%{user.domain}'
              if: ctx.user?.email != null
              on_failure:
                - append:
                    field: error.message
                    value: 'error message'
        """;

        String expected = """
        pipeline[apipeline] {
            loghub.processors.Dissect {
                field: [user email],
                pattern: "%{user.name}@%{user.domain}",
                if: [user email] == *,
                failure: (
                    [error message] =+ "error message"
                ),
            }
        }
        """;
        convert(esPipeline, expected);
    }

    @Test
    public void testDate() throws IOException {
        String esPipeline = """
          - date:
              field: date
              target_field: '@timestamp'
              formats:
                - E MMM dd HH:mm:ss yyyy
                - E MMM  d HH:mm:ss yyyy
                - E MMM d HH:mm:ss yyyy
                - yyyy-mm-dd HH:mm:ss
              timezone: '{{{event.timezone}}}'
        """;

        String expected = """
        pipeline[apipeline] {
            loghub.processors.DateParser {
                field: [date],
                destination: [@timestamp],
                patterns: ["E MMM dd HH:mm:ss yyyy", "E MMM  d HH:mm:ss yyyy", "E MMM d HH:mm:ss yyyy", "yyyy-mm-dd HH:mm:ss"],
                timezone: [event timezone],
            }
        }
        """;
        convert(esPipeline, expected);
    }

    @Test
    public void testUrldecode() throws IOException {
        String esPipeline = """
          - urldecode:
              field: url
        """;

        String expected = """
        pipeline[apipeline] {
            loghub.processors.DecodeUrl {
                field: [url],
            }
        }
        """;
        convert(esPipeline, expected);
    }

    @Test
    public void testConvert() throws IOException {
        String esPipeline = """
          - convert:
              field: source_ip
              target_field: source.ip
              type: ip
              ignore_missing: true
              if: ctx.source_ip != ''
          - convert:
              field: source_port
              target_field: source.port
              type: long
        """;

        String expected = """
        pipeline[apipeline] {
            loghub.processors.Convert {
                field: [source_ip],
                destination: [source ip],
                className: "java.net.InetAddress",
                if: [source_ip] != "",
            } |
            (java.lang.Long)[source_port] |
            [source port] < [source_port]
        }
        """;
        convert(esPipeline, expected);
    }

    @Test
    public void testForeach() throws IOException {
        String esPipeline = """
          - foreach:
              field: attribute
              ignore_missing: true
              if: ctx.attribute instanceof List
              processor:
                pipeline:
                  name: '{{ IngestPipeline "process" }}'
                append:
                  field: dest
                  value: 1
          - foreach:
              field: attribute
              processor:
                set:
                    field: attribute
                    value: 1
        """;

        String expected = """
        pipeline[apipeline] {
            [attribute] instanceof java.util.List ? foreach[attribute] (
                $process |
                [dest] =+ 1
            ) |
            foreach[attribute] (
                [attribute] = 1
            )
        }
        """;
        convert(esPipeline, expected);
    }

    @Test
    public void testJson() throws IOException {
        String esPipeline = """
          - json:
              field: event.original
              target_field: json
          - json:
              field: event.original
        """;

        String expected = """
        pipeline[apipeline] {
            path[json] (
                loghub.processors.ParseJson {
                    field: [. event original],
                }
            ) |
            loghub.processors.ParseJson {
                field: [event original],
            }
        }
        """;
        convert(esPipeline, expected);
    }

}
