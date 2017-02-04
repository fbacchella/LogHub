package org.logstash.beats;

public class Ack {

    private final byte protocol;
    private final long sequence;

    public Ack(byte protocol, long sequence) {
        this.protocol = protocol;
        this.sequence = sequence;
    }

    public byte getProtocol() {
        return protocol;
    }

    public long getSequence() {
        return sequence;
    }
}
