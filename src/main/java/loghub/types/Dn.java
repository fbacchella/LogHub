package loghub.types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;

import com.fasterxml.jackson.annotation.JsonValue;

import lombok.Getter;

public class Dn {

    @Getter
    private final LdapName name;

    public Dn(String name) {
        try {
            this.name = new LdapName(name);
        } catch (InvalidNameException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    @Override
    @JsonValue
    public String toString() {
        List<Rdn> parts = new ArrayList<>(name.getRdns());
        Collections.reverse(parts);
        return parts.stream().map(Rdn::toString).collect(Collectors.joining(", "));
    }

}
