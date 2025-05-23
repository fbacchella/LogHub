package loghub.configuration;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.cert.CertificateException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import loghub.Helpers;
import lombok.NonNull;

public class SecretsHandler implements Closeable {

    private enum ACTION {
        LOAD,
        CREATE,
    }

    private static final char[] NOPASSWORD = "".toCharArray();
    private static final KeyStore.PasswordProtection NOPROTECTION = new KeyStore.PasswordProtection(NOPASSWORD);

    private final KeyStore ks;
    private final URI storePath;
    private boolean modified = false;

    public static SecretsHandler load(@NonNull String storePath) throws IOException {
        return new SecretsHandler(Helpers.fileUri(storePath), ACTION.LOAD);
    }

    public static SecretsHandler create(@NonNull String storePath) throws IOException {
        return new SecretsHandler(Helpers.fileUri(storePath), ACTION.CREATE);
    }

    public static SecretsHandler empty() {
        return new SecretsHandler();
    }

    private SecretsHandler(URI storePath, ACTION action) throws IOException {
        this.storePath = storePath;
        switch (action) {
        case LOAD:
            ks = load();
            break;
        case CREATE:
            ks = create();
            modified = true;
            save(true);
            break;
         default:
             throw new IllegalStateException("not reachable code");
        }
    }

    private SecretsHandler() {
        this.storePath = null;
        try {
            ks = KeyStore.getInstance("JCEKS");
        } catch (KeyStoreException ex) {
            throw new IllegalStateException("Keystore environment unusable", ex);
        }

    }

    private KeyStore load() throws IOException {
        try {
            KeyStore tempks = KeyStore.getInstance("JCEKS");
            try (InputStream is = new BufferedInputStream(getReader())) {
                tempks.load(is, NOPASSWORD);
            }
            return tempks;
        } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException ex) {
            throw new IllegalStateException("Keystore environment unusable", ex);
        }
    }

    private InputStream getReader() throws IOException {
        try {
            // Try the simple embeded URL readers
            return storePath.toURL().openStream();
        } catch (MalformedURLException ex) {
            // Try improved file system providers
            try {
                return Files.newInputStream(Paths.get(storePath));
            } catch (FileSystemNotFoundException e) {
                throw new IllegalArgumentException("Unsupported secret source");
            }
        }
    }

    public void save() throws IOException {
        save(false);
    }

    private void save(boolean create) throws IOException {
        if (modified && storePath != null) {
            try {
                Path store = Paths.get(storePath);
                if (store.getFileSystem().isReadOnly()) {
                    throw new IllegalArgumentException("Can't create a secret store on a read-only storage");
                }
                if (create && Files.exists(store)) {
                    throw new IllegalArgumentException("Can't overwrite existing secret store");
                } else if (create) {
                    Set<PosixFilePermission> defaultPerms = new HashSet<>();
                    defaultPerms.add(PosixFilePermission.OWNER_READ);
                    defaultPerms.add(PosixFilePermission.OWNER_WRITE);
                    Files.createFile(store, PosixFilePermissions.asFileAttribute(defaultPerms));
                } else if (!Files.exists(store)) {
                    throw new IllegalStateException("Secret store vanished");
                }
                try (FileChannel fc = FileChannel.open(store, StandardOpenOption.WRITE);
                     OutputStream os = Channels.newOutputStream(fc)) {
                    ks.store(os, NOPASSWORD);
                }
                modified = false;
            } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException ex) {
                throw new IllegalStateException("Keystore environment unusable", ex);
            }
        }
    }

    private KeyStore create() throws IOException {
        try {
            KeyStore tempks = KeyStore.getInstance("JCEKS");
            tempks.load(null, NOPASSWORD);
            return tempks;
        } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException ex) {
            throw new IllegalStateException("Keystore environment unusable", ex);
        }
    }

    public void add(String alias, byte[] secret) {
        try {
            if (ks.containsAlias(alias)) {
                throw new IllegalArgumentException("Alias already exists, remove it before adding: " + alias);
            }
            SecretKey generatedSecret = new SecretKeySpec(secret, "RAW");
            KeyStore.PasswordProtection keyStorePP = new KeyStore.PasswordProtection(NOPASSWORD);
            ks.setEntry(alias, new KeyStore.SecretKeyEntry(generatedSecret), keyStorePP);
            modified = true;
        } catch (KeyStoreException ex) {
            throw new IllegalStateException("Keystore environment unusable", ex);
        }
    }

    public void delete(String alias) {
        try {
            ks.deleteEntry(alias);
            modified = true;
        } catch (KeyStoreException ex) {
            throw new IllegalStateException("Keystore environment unusable", ex);
        }
    }

    public byte[] get(String alias) {
        try {
            KeyStore.SecretKeyEntry ske = (KeyStore.SecretKeyEntry) ks.getEntry(alias, NOPROTECTION);
            if (ske == null) {
                throw new IllegalArgumentException("Missing alias " + alias);
            } else {
                byte[] buffer = ske.getSecretKey().getEncoded();
                return Arrays.copyOf(buffer, buffer.length);
            }
        } catch (NoSuchAlgorithmException | UnrecoverableEntryException | KeyStoreException ex) {
            throw new IllegalStateException("Keystore environment unusable", ex);
        }

    }

    public Stream<Map.Entry<String, KeyStore.SecretKeyEntry>> list() {
        try {
            return StreamSupport.stream(Helpers.enumIterable(ks.aliases()).spliterator(), false)
            .filter(a -> {
                try {
                    return ks.entryInstanceOf(a, KeyStore.SecretKeyEntry.class);
                } catch (KeyStoreException e) {
                    return false;
                }
            })
            .map(a -> {
                try {
                    KeyStore.SecretKeyEntry ske = (KeyStore.SecretKeyEntry) ks.getEntry(a, NOPROTECTION);
                    return new SimpleImmutableEntry<>(a, ske);
                } catch (NoSuchAlgorithmException | UnrecoverableEntryException | KeyStoreException e) {
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .map(e -> e); // Identify but compilation fails without it
        } catch (KeyStoreException ex) {
            throw new IllegalStateException("Keystore environment unusable", ex);
        }
    }

    @Override
    public void close() throws IOException {
        save();
    }

}
