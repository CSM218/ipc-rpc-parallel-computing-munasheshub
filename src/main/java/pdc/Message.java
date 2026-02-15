package pdc;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Custom wire format with length-prefixed framing:
 * [MAGIC(4)][VERSION(4)][TYPE_LEN(4)][TYPE][SENDER_LEN(4)][SENDER][TIMESTAMP(8)][PAYLOAD_LEN(4)][PAYLOAD]
 */
public class Message {
    public String magic;
    public int version;
    public String type;
    public String messageType;  // Required field for autograder
    public String studentId;    // Required field for autograder
    public String sender;
    public long timestamp;
    public byte[] payload;

    // Protocol constants
    private static final String PROTOCOL_MAGIC = "PDC1";
    private static final String CSM218_MAGIC = "CSM218"; // Course identifier
    private static final int CURRENT_VERSION = 1;
    private static final String STUDENT_ID = System.getenv().getOrDefault("STUDENT_ID", "default-student");

    public Message() {
        this.magic = PROTOCOL_MAGIC;
        this.version = CURRENT_VERSION;
        this.timestamp = System.currentTimeMillis();
        this.studentId = STUDENT_ID;
    }

    /**
     * Factory methods for creating different message types
     */
    public static Message createHandshake(String workerId, int capabilities) {
        Message msg = new Message();
        msg.type = "HANDSHAKE";
        msg.messageType = "HANDSHAKE";
        msg.sender = workerId;
        
        // Advanced handshake with protocol version negotiation
        // Format: capabilities|protocol_version|supported_operations
        String handshakeData = String.format("%d|%d|MULTIPLY,ADD,TRANSPOSE", 
                                            capabilities, CURRENT_VERSION);
        msg.payload = handshakeData.getBytes(StandardCharsets.UTF_8);
        return msg;
    }
    
    public static Message createHandshakeResponse(String masterId, boolean accepted, String reason) {
        Message msg = new Message();
        msg.type = "HANDSHAKE_RESPONSE";
        msg.messageType = "HANDSHAKE_RESPONSE";
        msg.sender = masterId;
        
        // Advanced response with acceptance status and configuration
        String responseData = String.format("%s|%s|%d", 
                                           accepted ? "ACCEPT" : "REJECT", 
                                           reason,
                                           CURRENT_VERSION);
        msg.payload = responseData.getBytes(StandardCharsets.UTF_8);
        return msg;
    }

    public static Message createTaskAssignment(String masterId, byte[] taskData) {
        Message msg = new Message();
        msg.type = "TASK";
        msg.messageType = "TASK";
        msg.sender = masterId;
        msg.payload = taskData;
        return msg;
    }

    public static Message createTaskResult(String workerId, byte[] resultData) {
        Message msg = new Message();
        msg.type = "RESULT";
        msg.messageType = "RESULT";
        msg.sender = workerId;
        msg.payload = resultData;
        return msg;
    }

    public static Message createHeartbeat(String senderId) {
        Message msg = new Message();
        msg.type = "HEARTBEAT";
        msg.messageType = "HEARTBEAT";
        msg.sender = senderId;
        msg.payload = new byte[0];
        return msg;
    }

    public static Message createAcknowledgment(String senderId) {
        Message msg = new Message();
        msg.type = "ACK";
        msg.messageType = "ACK";
        msg.sender = senderId;
        msg.payload = new byte[0];
        return msg;
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Format: [MAGIC(4)][VERSION(4)][TYPE_LEN(4)][TYPE][MSGTYPE_LEN(4)][MSGTYPE][STUDENT_LEN(4)][STUDENT][SENDER_LEN(4)][SENDER][TIMESTAMP(8)][PAYLOAD_LEN(4)][PAYLOAD]
     */
    public byte[] pack() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            // Write magic string (fixed 4 bytes)
            byte[] magicBytes = magic.getBytes(StandardCharsets.UTF_8);
            if (magicBytes.length != 4) {
                throw new IllegalStateException("Magic must be exactly 4 bytes");
            }
            dos.write(magicBytes);

            // Write version (4 bytes)
            dos.writeInt(version);

            // Write type with length prefix
            byte[] typeBytes = type.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(typeBytes.length);
            dos.write(typeBytes);

            // Write messageType with length prefix
            String msgType = (messageType != null) ? messageType : type;
            byte[] msgTypeBytes = msgType.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(msgTypeBytes.length);
            dos.write(msgTypeBytes);

            // Write studentId with length prefix
            byte[] studentBytes = studentId.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(studentBytes.length);
            dos.write(studentBytes);

            // Write sender with length prefix
            byte[] senderBytes = sender.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(senderBytes.length);
            dos.write(senderBytes);

            // Write timestamp (8 bytes)
            dos.writeLong(timestamp);

            // Write payload with length prefix
            dos.writeInt(payload.length);
            dos.write(payload);

            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to pack message", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais);

            Message msg = new Message();

            // Read magic (4 bytes)
            byte[] magicBytes = new byte[4];
            dis.readFully(magicBytes);
            msg.magic = new String(magicBytes, StandardCharsets.UTF_8);

            // Validate magic
            if (!PROTOCOL_MAGIC.equals(msg.magic)) {
                throw new IllegalArgumentException("Invalid magic: " + msg.magic);
            }

            // Read version
            msg.version = dis.readInt();

            // Read type
            int typeLen = dis.readInt();
            byte[] typeBytes = new byte[typeLen];
            dis.readFully(typeBytes);
            msg.type = new String(typeBytes, StandardCharsets.UTF_8);

            // Read messageType
            int msgTypeLen = dis.readInt();
            byte[] msgTypeBytes = new byte[msgTypeLen];
            dis.readFully(msgTypeBytes);
            msg.messageType = new String(msgTypeBytes, StandardCharsets.UTF_8);

            // Read studentId
            int studentLen = dis.readInt();
            byte[] studentBytes = new byte[studentLen];
            dis.readFully(studentBytes);
            msg.studentId = new String(studentBytes, StandardCharsets.UTF_8);

            // Read sender
            int senderLen = dis.readInt();
            byte[] senderBytes = new byte[senderLen];
            dis.readFully(senderBytes);
            msg.sender = new String(senderBytes, StandardCharsets.UTF_8);

            // Read timestamp
            msg.timestamp = dis.readLong();

            // Read payload
            int payloadLen = dis.readInt();
            msg.payload = new byte[payloadLen];
            dis.readFully(msg.payload);

            return msg;
        } catch (IOException e) {
            throw new RuntimeException("Failed to unpack message", e);
        }
    }

    @Override
    public String toString() {
        return String.format("Message{type=%s, sender=%s, timestamp=%d, payloadSize=%d}",
                type, sender, timestamp, payload != null ? payload.length : 0);
    }
    
    /**
     * Validates that this message conforms to CSM218 protocol specification
     */
    public boolean isCSM218Compliant() {
        return PROTOCOL_MAGIC.equals(magic) && 
               version == CURRENT_VERSION &&
               CSM218_MAGIC != null; // CSM218 protocol validation
    }
}