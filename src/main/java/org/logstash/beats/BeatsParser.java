package org.logstash.beats;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

public class BeatsParser extends ByteToMessageDecoder {
    public final static ObjectMapper MAPPER = new ObjectMapper().registerModule(new AfterburnerModule());
    private final static Logger logger = LogManager.getLogger();
    private static final Charset UTF8 = Charset.forName("UTF8");

    private Batch batch = new Batch();

    private enum States {
        READ_HEADER(1),
        READ_FRAME_TYPE(1),
        READ_WINDOW_SIZE(4),
        READ_JSON_HEADER(8),
        READ_COMPRESSED_FRAME_HEADER(4),
        READ_COMPRESSED_FRAME(-1), // -1 means the length to read is variable and defined in the frame itself.
        READ_JSON(-1),
        READ_DATA_FIELDS(-1);

        private final int length;

        States(int length) {
            this.length = length;
        }

    }

    private States currentState = States.READ_HEADER;
    private int requiredBytes = States.READ_HEADER.length;
    private long sequence = 0;
    private final int maxPayloadSize;

    public BeatsParser(int maxPayloadSize) {
        super();
        this.maxPayloadSize = maxPayloadSize;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws IOException {
        if (!hasEnoughBytes(in)) {
            return;
        }

        switch (currentState) {
        case READ_HEADER: {
            logger.debug("Running: READ_HEADER");

            byte currentVersion = in.readByte();

            if (Protocol.isVersion2(currentVersion)) {
                logger.debug("Frame version 2 detected");
                batch.setProtocol(Protocol.VERSION_2);
            } else {
                logger.debug("Frame version 1 detected");
                batch.setProtocol(Protocol.VERSION_1);
            }

            transition(States.READ_FRAME_TYPE);
            break;
        }
        case READ_FRAME_TYPE: {
            byte frameType = in.readByte();

            switch (frameType) {
            case Protocol.CODE_WINDOW_SIZE: {
                transition(States.READ_WINDOW_SIZE);
                break;
            }
            case Protocol.CODE_JSON_FRAME: {
                // Reading Sequence + size of the payload
                transition(States.READ_JSON_HEADER);
                break;
            }
            case Protocol.CODE_COMPRESSED_FRAME: {
                transition(States.READ_COMPRESSED_FRAME_HEADER);
                break;
            }
            case Protocol.CODE_FRAME: {
                transition(States.READ_DATA_FIELDS);
                break;
            }
            default: {
                throw new InvalidFrameProtocolException("Invalid Frame Type, received: " + frameType);
            }
            }
            break;
        }
        case READ_WINDOW_SIZE: {
            logger.debug("Running: READ_WINDOW_SIZE");

            batch.setBatchSize(in.readUnsignedInt());

            // This is unlikely to happen but I have no way to known when a frame is
            // actually completely done other than checking the windows and the sequence number,
            // If the FSM read a new window and I have still
            // events buffered I should send the current batch down to the next handler.
            if (!batch.isEmpty()) {
                logger.warn("New window size received but the current batch was not complete, sending the current batch");
                out.add(batch);
                batchComplete();
            }

            transition(States.READ_HEADER);
            break;
        }
        case READ_DATA_FIELDS: {
            // Lumberjack version 1 protocol, which use the Key:Value format.
            logger.debug("Running: READ_DATA_FIELDS");
            sequence = in.readUnsignedInt();
            int fieldsCount = (int) in.readUnsignedInt();
            int count = 0;

            if (fieldsCount <= 0) {
                throw new InvalidFrameProtocolException("Invalid number of fields, received: " + fieldsCount);
            }

            Map<String, String> dataMap = new HashMap<>((int)fieldsCount);

            while(count < fieldsCount) {
                int fieldLength = (int) in.readUnsignedInt();
                if (fieldLength > in.readableBytes() || fieldLength <= 0) {
                    throw new InvalidFrameProtocolException("Invalid field length, received: " + fieldLength);
                }
                ByteBuf fieldBuf = in.readBytes(fieldLength);
                String field = fieldBuf.toString(UTF8);
                fieldBuf.release();

                int dataLength = (int) in.readUnsignedInt();
                if(dataLength > in.readableBytes() || dataLength <= 0) {
                    throw new InvalidFrameProtocolException("Invalid data length length, received: " + dataLength);
                }
                ByteBuf dataBuf = in.readBytes(dataLength);
                String data = dataBuf.toString(UTF8);
                dataBuf.release();

                dataMap.put(field, data);

                count++;
            }

            Message message = new Message(sequence, dataMap);
            batch.addMessage(message);

            if(batch.complete()) {
                out.add(batch);
                batchComplete();
            }

            transition(States.READ_HEADER);

            break;
        }
        case READ_JSON_HEADER: {
            logger.debug("Running: READ_JSON_HEADER");

            sequence = in.readUnsignedInt();
            // Overflow will return a negative value
            int jsonPayloadSize = (int) in.readUnsignedInt();
            if (jsonPayloadSize <= 0) {
                throw new InvalidFrameProtocolException("Invalid json length, received: " + jsonPayloadSize);
            }

            transition(States.READ_JSON, jsonPayloadSize);
            break;
        }
        case READ_COMPRESSED_FRAME_HEADER: {
            logger.debug("Running: READ_COMPRESSED_FRAME_HEADER");

            // Overflow will return a negative value
            int compressedPayloadSize = (int) in.readUnsignedInt();
            if (compressedPayloadSize <= 0) {
                throw new InvalidFrameProtocolException("Invalid compressed paylog length, received: " + compressedPayloadSize);
            }

            transition(States.READ_COMPRESSED_FRAME, compressedPayloadSize);
            break;
        }
        case READ_COMPRESSED_FRAME: {
            logger.debug("Running: READ_COMPRESSED_FRAME");
            // Use the compressed size as the safe start for the buffer.
            ByteBuf buffer = ctx.alloc().buffer(requiredBytes);
            try (
                    ByteBufOutputStream buffOutput = new ByteBufOutputStream(buffer);
                    InflaterOutputStream inflater = new InflaterOutputStream(buffOutput, new Inflater());
                    ) {
                in.readBytes(inflater, requiredBytes);
                transition(States.READ_HEADER);
                try {
                    while (buffer.readableBytes() > 0) {
                        decode(ctx, buffer, out);
                    }
                } finally {
                    buffer.release();
                }
            }

            break;
        }
        case READ_JSON: {
            logger.debug("Running: READ_JSON");

            byte[] bytes = new byte[requiredBytes];
            in.readBytes(bytes);
            @SuppressWarnings("unchecked")
            Message message = new Message(sequence, (Map<String, String>) MAPPER.readValue(bytes, Object.class));

            batch.addMessage(message);

            if(batch.size() == batch.getBatchSize()) {
                logger.debug("Sending batch size: {}, windowSize: {} , seq: ", () -> batch.size(), () -> batch.getBatchSize(), () -> sequence);

                out.add(batch);
                batchComplete();
            }

            transition(States.READ_HEADER);
            break;
        }
        }
    }

    private boolean hasEnoughBytes(ByteBuf in) {
        return in.readableBytes() >= requiredBytes;
    }

    private void transition(States next) throws InvalidFrameProtocolException {
        transition(next, next.length);
    }

    private void transition(States nextState, int requiredBytes) throws InvalidFrameProtocolException {
        logger.debug("Transition, from: {}, to: {}, requiring ()  bytes", currentState, nextState, requiredBytes);
        if (requiredBytes > maxPayloadSize) {
            throw new InvalidFrameProtocolException("Oversized payload: " + requiredBytes);
        }
        this.currentState = nextState;
        this.requiredBytes = requiredBytes;
    }

    private void batchComplete() {
        requiredBytes = 0;
        sequence = 0;
        batch = new Batch();
    }

    public class InvalidFrameProtocolException extends IOException {
        InvalidFrameProtocolException(String message) {
            super(message);
        }
    }

}
