package loghub.cbor;

import java.io.IOException;
import java.net.URI;

import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;

public class URITagHandler extends CborTagHandler<URI> {

    public URITagHandler() {
        super(32, URI.class);
    }

    @Override
    public URI parse(CBORParser p) throws IOException {
        return java.net.URI.create(p.getText());
    }

    @Override
    public void write(URI data, CBORGenerator p) throws IOException {
        p.writeString(data.toString());
    }

}
