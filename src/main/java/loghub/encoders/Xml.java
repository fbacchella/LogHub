package loghub.encoders;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.jackson.JacksonBuilder;
import lombok.Setter;

@BuilderClass(Xml.Builder.class)
@CanBatch
public class Xml extends AbstractJacksonEncoder<Xml.Builder> {

    public static class Builder extends AbstractJacksonEncoder.Builder<Xml> {
        @Setter
        private boolean pretty = false;
        @Setter
        private boolean dateAsText = false;
        @Setter
        private boolean xmlDeclaration = true;
        @Override
        public Xml build() {
            return new Xml(this);
        }
    };
    public static Builder getBuilder() {
        return new Builder();
    }

    private Xml(Builder builder) {
        super(builder);
    }

    @Override
    protected JacksonBuilder<?> getWriterBuilder(Builder builder) {
        JacksonBuilder<XmlMapper> jbuilder = JacksonBuilder.get(XmlMapper.class)
                .setConfigurator(om -> {
                    om.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, ! (builder.pretty || builder.dateAsText));
                    om.configure(ToXmlGenerator.Feature.WRITE_XML_DECLARATION, builder.xmlDeclaration);
                })
                ;
        if (builder.pretty) {
            jbuilder.feature(SerializationFeature.INDENT_OUTPUT);
        }
        return jbuilder;
    }

}
