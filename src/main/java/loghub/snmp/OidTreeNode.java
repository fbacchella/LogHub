package loghub.snmp;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.swing.tree.DefaultMutableTreeNode;

import org.snmp4j.smi.OID;

public class OidTreeNode extends DefaultMutableTreeNode {

    static {
    }
    private final String name;
    private final Map<Integer, OidTreeNode> childs = new HashMap<Integer, OidTreeNode>();
    private final OID oid;

    public OidTreeNode() {
        super(null);
        oid = null;
        name = null;
    }
    private OidTreeNode(OidTreeNode parent, int id, String name) {
        super(id);
        this.name = name;
        if(parent.oid == null) {
            this.oid = new OID(new int[] {id});
        }  else {
            this.oid = new OID(parent.getOID().getValue(), id);
        }
        parent.add(this);
    }
    public void addOID(OID oid, String name) {
        int[] elements = oid.getValue();

        OID oidParent = new OID(Arrays.copyOf(elements, elements.length - 1));
        OidTreeNode parent = ((OidTreeNode) getRoot()).search(oidParent);
        new OidTreeNode(parent, elements[elements.length - 1], name);
    }
    public OidTreeNode AddChild(OID oidParent, int id, String name) {
        OidTreeNode parent = ((OidTreeNode) getRoot()).search(oidParent);
        return new OidTreeNode(parent, id, name);
    }
    public String getName() {
        return name;
    }
    public OidTreeNode search(OID oid) {
        return search(oid, 0);
    }
    public OidTreeNode search(OID oid, int level) {
        if(oid.size() == level) {
            return this;
        }
        Integer key = oid.get(level);
        if (childs.containsKey(key)) {
            return childs.get(key).search(oid, level + 1);
        }
        return this;
    }
    public void add(OidTreeNode newChild) {
        childs.put((Integer)newChild.getUserObject(), newChild);
        super.add(newChild);
    }
    @Override
    public String toString() {
        return oid + ": " + name;
    }
    public OID getOID() {
        return oid;
    }

    public static void main(String[] args) {
        OidTreeNode top = new OidTreeNode();

        SortedMap<String, String> oids = new TreeMap<String, String>(new NaturalOrderComparator());

        InputStream in = OidTreeNode.class.getClassLoader().getResourceAsStream("oid.properties");
        Properties p = new Properties();
        try {
            p.load(in);
            for(Entry<Object, Object> e: p.entrySet()) {
                oids.put((String) e.getKey(), (String) e.getValue());
            }
            for(Entry<String, String> e: oids.entrySet()) {
                top.addOID(new OID(e.getKey()), e.getValue());                
            }
        } catch (IOException e) {
        }
        @SuppressWarnings("unchecked")
        Enumeration<OidTreeNode>  i = top.depthFirstEnumeration();
        while(i.hasMoreElements()) {
            System.out.println(i.nextElement());
        }
    }
}
