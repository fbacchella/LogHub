package loghub.encoders;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.jackson.JacksonBuilder;
import loghub.types.MimeType;
import lombok.Setter;

@BuilderClass(Xml.Builder.class)
@CanBatch
public class Xml extends AbstractJacksonEncoder<Xml.Builder, XmlMapper> {

    public static final MimeType MIME_TYPE = MimeType.of("application/xml");

    @Setter
    public static class Builder extends AbstractJacksonEncoder.Builder<Xml> {
        private boolean pretty = false;
        private boolean dateAsText = false;
        private boolean xmlDeclaration = true;
        @Override
        public Xml build() {
            return new Xml(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private Xml(Builder builder) {
        super(builder);
    }

    @Override
    protected JacksonBuilder<XmlMapper> getWriterBuilder(Builder builder) {
        JacksonBuilder<XmlMapper> jbuilder = JacksonBuilder.get(XmlMapper.class)
                .setConfigurator(om -> {
                    om.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, ! (builder.pretty || builder.dateAsText));
                    om.configure(ToXmlGenerator.Feature.WRITE_XML_DECLARATION, builder.xmlDeclaration);
                });
        if (builder.pretty) {
            jbuilder.feature(SerializationFeature.INDENT_OUTPUT);
        }
        return jbuilder;
    }

    @Override
    public MimeType getMimeType() {
        return MIME_TYPE;
    }

}
