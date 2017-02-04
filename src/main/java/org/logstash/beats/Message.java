package org.logstash.beats;

import java.util.HashMap;
import java.util.Map;

public class Message implements Comparable<Message> {
    private final long sequence;
    private String identityStream;
    private final Map<?, ?> data;
    private Batch batch;

    public Message(long sequence, Map<?, ?> map) {
        this.sequence = sequence;
        this.data = map;

        identityStream = extractIdentityStream();
    }

    public long getSequence() {
        return sequence;
    }

    public Map<?, ?> getData() {
        return data;
    }

    @Override
    public int compareTo(Message o) {
        return Long.compare(getSequence(), o.getSequence());
    }

    public Batch getBatch() {
        return batch;
    }

    public void setBatch(Batch newBatch) {
        batch = newBatch;
    }

    public String getIdentityStream() {
        return identityStream;
    }

    private String extractIdentityStream() {
        @SuppressWarnings("unchecked")
        Map<String, String> beatsData = (HashMap<String, String>) getData().get("beat");

        if (beatsData != null) {
            String id = beatsData.get("id");
            String resourceId = beatsData.get("resource_id");

            if (id != null && resourceId != null) {
                return id + "-" + resourceId;
            } else {
                return beatsData.get("name").toString() + "-" + beatsData.get("source");
            }
        } else {
            return null;
        }

    }
}
