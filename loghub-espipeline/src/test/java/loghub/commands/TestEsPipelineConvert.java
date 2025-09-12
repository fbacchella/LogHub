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
import loghub.jackson.JacksonBuilder;

public class TestEsPipelineConvert {


    private String convert(String esPipeline, String expected) throws IOException {
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
        System.err.println(converted);
        Configuration.parse(new StringReader(converted));
        Assert.assertEquals(expected, converted);
        return converted;
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

}
