package loghub.zmq;

import java.nio.file.Path;
import java.security.Key;
import java.security.KeyStore;
import java.util.Collections;
import java.util.Map;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import loghub.security.ssl.MultiKeyStoreProvider;
import lombok.ToString;

@Parameters(commandNames = {"zmqkey"})
@ToString
public class KeyTool {

    @SuppressWarnings("CanBeFinal")
    @Parameter(names = {"--import"}, description = "A key to import")
    private Path keypath = null;

    @SuppressWarnings("CanBeFinal")
    @Parameter(names = {"--name"}, description = "A key to import")
    private String keyname = ZMQSocketFactory.KEYNAME;

    @Parameter(names = {"--exportsecret"}, description = "Export secret as a p8 file")
    private Path p8path = null;
    public void process(String configFile) {
        try {
            Map<String, Object> props = Collections.emptyMap(); //Configuration.readProperties(configFile);

            if (keypath != null) {
                MultiKeyStoreProvider.SubKeyStore param = new MultiKeyStoreProvider.SubKeyStore();
                param.addSubStore(keypath.normalize().toString());
                KeyStore ks = KeyStore.getInstance(MultiKeyStoreProvider.NAME, MultiKeyStoreProvider.PROVIDERNAME);
                ks.load(param);
                KeyStore.Entry e = ks.getEntry(keyname, null);
                Key key = ks.getKey(keyname, new char[]{});

                //ZMQHelper.NACLKEYFACTORY.getKeySpec(e.)
            }
            System.err.println(props);
            /*Properties props = Configuration.parse(configFile);
            System.err.println(props.getZMQSocketFactory().getKeyEntry());
            System.err.println(props.getZMQSocketFactory().getKeyEntry().getAttributes());
            System.err.println(props.getZMQSocketFactory().getKeyEntry().getPrivateKey().getAlgorithm());
            System.err.println(props.getZMQSocketFactory().getKeyEntry().getPrivateKey().getFormat());
            Files.write(Paths.get("/tmp/cert.p8"), props.getZMQSocketFactory().getKeyEntry().getPrivateKey().getEncoded());*/
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
