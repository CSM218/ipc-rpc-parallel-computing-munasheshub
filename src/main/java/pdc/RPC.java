package pdc;

import java.io.*;
import java.net.Socket;

/**
 * RPC (Remote Procedure Call) abstraction layer.
 * Provides high-level method invocation semantics over the socket layer.
 * 
 * This class abstracts away the low-level socket and message details,
 * allowing callers to invoke remote methods as if they were local.
 */
public class RPC {
    
    private Socket socket;
    private DataOutputStream out;
    private DataInputStream in;
    
    /**
     * Create an RPC connection to a remote endpoint
     */
    public RPC(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
    }
    
    /**
     * Create an RPC handler from an existing socket connection
     */
    public RPC(Socket socket) throws IOException {
        this.socket = socket;
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
    }
    
    /**
     * Invoke a remote procedure call
     * 
     * @param method The method name to invoke
     * @param args The arguments to pass
     * @return The result from the remote method
     */
    public Object call(String method, Object... args) throws IOException, ClassNotFoundException {
        // Create RPC request message
        Message request = new Message();
        request.type = "RPC_CALL";
        request.messageType = "RPC_CALL";
        request.sender = "rpc-client";
        
        // Serialize method and arguments
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeUTF(method);
        oos.writeInt(args.length);
        for (Object arg : args) {
            oos.writeObject(arg);
        }
        oos.flush();
        request.payload = baos.toByteArray();
        
        // Send request
        sendMessage(request);
        
        // Receive response
        Message response = receiveMessage();
        
        // Deserialize result
        ByteArrayInputStream bais = new ByteArrayInputStream(response.payload);
        ObjectInputStream ois = new ObjectInputStream(bais);
        return ois.readObject();
    }
    
    /**
     * Invoke a remote procedure without expecting a return value
     */
    public void callAsync(String method, Object... args) throws IOException {
        Message request = new Message();
        request.type = "RPC_CALL_ASYNC";
        request.messageType = "RPC_CALL_ASYNC";
        request.sender = "rpc-client";
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeUTF(method);
        oos.writeInt(args.length);
        for (Object arg : args) {
            oos.writeObject(arg);
        }
        oos.flush();
        request.payload = baos.toByteArray();
        
        sendMessage(request);
    }
    
    /**
     * Handle an incoming RPC call (server side)
     */
    public static class Handler {
        private RPC rpc;
        
        public Handler(RPC rpc) {
            this.rpc = rpc;
        }
        
        /**
         * Receive and handle an RPC call
         */
        public RPCRequest receive() throws IOException, ClassNotFoundException {
            Message request = rpc.receiveMessage();
            
            if ("RPC_CALL".equals(request.type) || "RPC_CALL_ASYNC".equals(request.type)) {
                ByteArrayInputStream bais = new ByteArrayInputStream(request.payload);
                ObjectInputStream ois = new ObjectInputStream(bais);
                
                String method = ois.readUTF();
                int argCount = ois.readInt();
                Object[] args = new Object[argCount];
                for (int i = 0; i < argCount; i++) {
                    args[i] = ois.readObject();
                }
                
                return new RPCRequest(method, args, "RPC_CALL_ASYNC".equals(request.type));
            }
            
            return null;
        }
        
        /**
         * Send a response back to the caller
         */
        public void respond(Object result) throws IOException {
            Message response = new Message();
            response.type = "RPC_RESPONSE";
            response.messageType = "RPC_RESPONSE";
            response.sender = "rpc-server";
            
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(result);
            oos.flush();
            response.payload = baos.toByteArray();
            
            rpc.sendMessage(response);
        }
    }
    
    /**
     * Represents an RPC request
     */
    public static class RPCRequest {
        public String method;
        public Object[] args;
        public boolean isAsync;
        
        public RPCRequest(String method, Object[] args, boolean isAsync) {
            this.method = method;
            this.args = args;
            this.isAsync = isAsync;
        }
    }
    
    /**
     * Send a message over the RPC connection
     */
    private synchronized void sendMessage(Message msg) throws IOException {
        byte[] data = msg.pack();
        out.writeInt(data.length);
        out.write(data);
        out.flush();
    }
    
    /**
     * Receive a message from the RPC connection
     */
    private Message receiveMessage() throws IOException {
        int length = in.readInt();
        byte[] data = new byte[length];
        in.readFully(data);
        return Message.unpack(data);
    }
    
    /**
     * Close the RPC connection
     */
    public void close() throws IOException {
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }
}