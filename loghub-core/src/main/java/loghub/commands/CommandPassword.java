package loghub.commands;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import loghub.Helpers;
import loghub.configuration.SecretsHandler;
import lombok.ToString;
@Parameters(commandNames = { "secrets" })
@ToString
public class CommandPassword implements CommandRunner {

   // Secret sources
    @Parameter(names = { "--secret", "-S" }, description = "Secret")
    String secretValue = null;
    @Parameter(names = { "--file", "-f" }, description = "Password file")
    String fromFile = null;
    @Parameter(names = { "--console", "-c" }, description = "Read from console")
    boolean fromConsole = false;
    @SuppressWarnings("CanBeFinal")
    @Parameter(names = { "--stdin", "-i" }, description = "Read from stdin")
    boolean fromStdin = false;

    // Identification elements
    @Parameter(names = { "--alias", "-a" }, description = "Secret entry alias")
    String alias = null;
    @Parameter(names = { "--store", "-s" }, description = "The store file", required = true)
    String storeFile = null;

    // Actions
    @Parameter(names = { "--add" }, description = "Add a secret")
    boolean add = false;
    @Parameter(names = { "--del" }, description = "Delete a secret")
    boolean delete = false;
    @SuppressWarnings("CanBeFinal")
    @Parameter(names = { "--list" }, description = "List secrets")
    boolean list = false;
    @Parameter(names = { "--create" }, description = "Create te store file")
    boolean create = false;

    private byte[] readSecret() throws IOException {
        byte[] secret;
        if (fromConsole) {
            secret = new String(System.console().readPassword()).getBytes(StandardCharsets.UTF_8);
        } else if (secretValue != null) {
            secret = secretValue.getBytes(StandardCharsets.UTF_8);
        } else if (fromStdin) {
            ByteBuf buffer = Unpooled.buffer();
            byte[] readbuffer = new byte[256];
            while (System.in.read(readbuffer) > 0) {
                buffer.writeBytes(readbuffer);
            }
            secret = new byte[buffer.readableBytes()];
            buffer.readBytes(secret);
        } else if (fromFile != null) {
            secret = Files.readAllBytes(Paths.get(fromFile));
        } else {
            throw new IllegalStateException("No secret source defined");
        }
        return secret;
    }

    @Override
    public int run(List<String> unknownOptions) {
        try {
            if ((add ? 1 : 0) + (delete ? 1 : 0) + (list ? 1 : 0) + (create ? 1 : 0) != 1) {
                throw new IllegalStateException("A single action is required");
            }
            if ((fromConsole ? 1 : 0) + (fromStdin ? 1 : 0) + (secretValue != null ? 1 : 0) + (fromFile != null ?
                                                                                                       1 :
                                                                                                       0) > 1) {
                throw new IllegalStateException("Multiple secret sources given, pick one");
            }
            if ((fromConsole ? 1 : 0) + (fromStdin ? 1 : 0) + (secretValue != null ? 1 : 0) + (fromFile != null ?
                                                                                                       1 :
                                                                                                       0) == 0) {
                // The default input is console
                fromConsole = true;
            }
            if (add) {
                try (SecretsHandler sh = SecretsHandler.load(storeFile)) {
                    sh.add(alias, readSecret());
                }
            } else if (delete) {
                try (SecretsHandler sh = SecretsHandler.load(storeFile)) {
                    sh.delete(alias);
                }
            } else if (list) {
                try (SecretsHandler sh = SecretsHandler.load(storeFile)) {
                    sh.list().map(Map.Entry::getKey).forEach(System.out::println);
                }
            } else if (create) {
                try (SecretsHandler ignored = SecretsHandler.create(storeFile)) {
                    // Nothing to do
                }
            }
            return 0;
        } catch (IOException | IllegalArgumentException ex) {
            System.err.println("Secret store operation failed: " + Helpers.resolveThrowableException(ex));
            return ExitCode.OPERATIONFAILED;
        } catch (IllegalStateException ex) {
            System.err.println("Secret store state broken: " + Helpers.resolveThrowableException(ex));
            ex.printStackTrace();
            return ExitCode.OPERATIONFAILED;
        }
    }

}
