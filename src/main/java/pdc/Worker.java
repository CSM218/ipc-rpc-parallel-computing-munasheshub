package pdc;

import java.io.*;
import java.net.Socket;
import java.net.ConnectException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * Features:
 * - Internal thread pool for parallel task execution
 * - Non-blocking communication with Master
 * - Atomic task execution with race condition prevention
 * - Heartbeat mechanism for cluster health monitoring
 * - Graceful failure handling for network issues
 */
public class Worker {
    
    private String workerId;
    private int capabilities;
    private ExecutorService taskExecutor;
    private ExecutorService communicationExecutor;
    private ScheduledExecutorService heartbeatExecutor;
    private final AtomicBoolean running;
    private final AtomicBoolean connected;
    private final AtomicInteger tasksCompleted;
    private final BlockingQueue<TaskBlock> taskQueue;
    
    private Socket masterSocket;
    private DataOutputStream out;
    private DataInputStream in;
    
    // Configuration from environment variables
    private static final int TASK_POOL_SIZE = Integer.parseInt(
        System.getenv().getOrDefault("WORKER_THREADS", 
                                     String.valueOf(Runtime.getRuntime().availableProcessors())));
    private static final int HEARTBEAT_INTERVAL_MS = Integer.parseInt(
        System.getenv().getOrDefault("HEARTBEAT_INTERVAL", "5000"));
    private static final int TASK_QUEUE_CAPACITY = Integer.parseInt(
        System.getenv().getOrDefault("TASK_QUEUE_SIZE", "100"));
    private static final int CONNECTION_TIMEOUT_MS = Integer.parseInt(
        System.getenv().getOrDefault("CONNECTION_TIMEOUT", "5000"));
    
    /**
     * Default constructor (for testing)
     */
    public Worker() {
        this("worker-" + System.currentTimeMillis());
    }
    
    /**
     * Constructor with worker ID
     */
    public Worker(String workerId) {
        this.workerId = workerId;
        this.capabilities = TASK_POOL_SIZE;
        this.running = new AtomicBoolean(false);
        this.connected = new AtomicBoolean(false);
        this.tasksCompleted = new AtomicInteger(0);
        this.taskQueue = new LinkedBlockingQueue<>(TASK_QUEUE_CAPACITY);
    }
    
    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake exchanges 'Identity' and 'Capability' sets.
     * 
     * Handles network failures gracefully - does not throw exceptions.
     */
    public void joinCluster(String masterHost, int port) {
        try {
            System.out.println("[Worker " + workerId + "] Attempting to connect to master at " + masterHost + ":" + port);
            
            // Attempt to establish connection with timeout
            masterSocket = new Socket();
            masterSocket.connect(new java.net.InetSocketAddress(masterHost, port), CONNECTION_TIMEOUT_MS);
            masterSocket.setSoTimeout(30000);
            
            out = new DataOutputStream(new BufferedOutputStream(masterSocket.getOutputStream()));
            in = new DataInputStream(new BufferedInputStream(masterSocket.getInputStream()));
            
            // Send handshake
            Message handshake = Message.createHandshake(workerId, capabilities);
            sendMessage(handshake);
            
            // Wait for handshake response
            Message response = receiveMessage();
            if ("HANDSHAKE_RESPONSE".equals(response.type)) {
                String responseData = new String(response.payload, java.nio.charset.StandardCharsets.UTF_8);
                String[] parts = responseData.split("\\|");
                if (parts.length >= 1 && "ACCEPT".equals(parts[0])) {
                    System.out.println("[Worker " + workerId + "] Handshake accepted");
                } else {
                    System.err.println("[Worker " + workerId + "] Handshake rejected: " + 
                                     (parts.length > 1 ? parts[1] : "unknown reason"));
                    closeConnection();
                    return;
                }
            } else if (!"ACK".equals(response.type)) {
                System.err.println("[Worker " + workerId + "] Expected HANDSHAKE_RESPONSE or ACK, got: " + response.type);
                closeConnection();
                return;
            }
            
            connected.set(true);
            System.out.println("[Worker " + workerId + "] Successfully joined cluster");
            
            // Initialize thread pools
            initializeThreadPools();
            
            // Start worker threads
            running.set(true);
            startMessageListener();
            startHeartbeat();
            startTaskProcessor();
            
        } catch (ConnectException e) {
            System.err.println("[Worker " + workerId + "] Could not connect to master: Connection refused");
            System.err.println("[Worker " + workerId + "] Master may not be running or network is unavailable");
        } catch (Exception e) {
            System.err.println("[Worker " + workerId + "] Failed to join cluster: " + e.getMessage());
        } finally {
            if (!connected.get()) {
                closeConnection();
            }
        }
    }
    
    /**
     * Initialize thread pools
     */
    private void initializeThreadPools() {
        if (taskExecutor == null) {
            taskExecutor = Executors.newFixedThreadPool(TASK_POOL_SIZE);
        }
        if (communicationExecutor == null) {
            communicationExecutor = Executors.newFixedThreadPool(2);
        }
        if (heartbeatExecutor == null) {
            heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
        }
    }
    
    /**
     * Executes a received task block.
     * 
     * This is a non-blocking invocation of the processing loop.
     * If not connected, returns immediately without throwing exceptions.
     * 
     * Students must ensure:
     * 1. The operation is atomic from the perspective of the Master.
     * 2. Overlapping tasks do not cause race conditions.
     * 3. 'End-to-End' logs are precise for performance instrumentation.
     */
    public void execute() {
        if (!connected.get()) {
            System.out.println("[Worker " + workerId + "] Not connected to cluster");
            return;
        }
        
        System.out.println("[Worker " + workerId + "] Active - " + 
                          taskQueue.size() + " queued, " + 
                          tasksCompleted.get() + " completed");
    }
    
    private void startMessageListener() {
        communicationExecutor.submit(() -> {
            while (running.get() && connected.get()) {
                try {
                    Message msg = receiveMessage();
                    handleMessage(msg);
                } catch (Exception e) {
                    if (running.get()) {
                        System.err.println("[Worker " + workerId + "] Connection lost");
                    }
                    break;
                }
            }
        });
    }
    
    private void handleMessage(Message msg) {
        switch (msg.type) {
            case "TASK":
                handleTaskAssignment(msg);
                break;
            case "SHUTDOWN":
                shutdown();
                break;
            case "HEARTBEAT":
                sendMessage(Message.createHeartbeat(workerId));
                break;
        }
    }
    
    private void handleTaskAssignment(Message msg) {
        try {
            TaskBlock task = TaskBlock.deserialize(msg.payload);
            task.assignedWorker = workerId;
            task.receiveTime = System.currentTimeMillis();
            
            if (!taskQueue.offer(task, 1, TimeUnit.SECONDS)) {
                System.err.println("[Worker " + workerId + "] Queue full, rejecting task " + task.taskId);
            }
        } catch (Exception e) {
            System.err.println("[Worker " + workerId + "] Failed to queue task: " + e.getMessage());
        }
    }
    
    private void startTaskProcessor() {
        communicationExecutor.submit(() -> {
            while (running.get() && connected.get()) {
                try {
                    TaskBlock task = taskQueue.poll(1, TimeUnit.SECONDS);
                    if (task != null) {
                        taskExecutor.submit(() -> executeTask(task));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
    }
    
    private void executeTask(TaskBlock task) {
        long startTime = System.nanoTime();
        
        try {
            task.result = performMatrixOperation(task);
            task.status = TaskStatus.COMPLETED;
            task.completionTime = System.currentTimeMillis();
            
            long endTime = System.nanoTime();
            double executionTimeMs = (endTime - startTime) / 1_000_000.0;
            
            System.out.printf("[Worker %s] Completed task %d in %.2f ms%n", 
                    workerId, task.taskId, executionTimeMs);
            
            if (connected.get()) {
                Message resultMsg = Message.createTaskResult(workerId, task.serialize());
                sendMessage(resultMsg);
            }
            
            tasksCompleted.incrementAndGet();
            
        } catch (Exception e) {
            System.err.println("[Worker " + workerId + "] Task " + task.taskId + " failed: " + e.getMessage());
            task.status = TaskStatus.FAILED;
            task.error = e.getMessage();
        }
    }
    
    private int[][] performMatrixOperation(TaskBlock task) {
        switch (task.operation) {
            case "BLOCK_MULTIPLY":
                return multiplyMatrixBlocks(task.matrixA, task.matrixB);
            case "BLOCK_ADD":
                return addMatrixBlocks(task.matrixA, task.matrixB);
            case "TRANSPOSE":
                return transposeMatrix(task.matrixA);
            default:
                throw new UnsupportedOperationException("Unknown operation: " + task.operation);
        }
    }
    
    private int[][] multiplyMatrixBlocks(int[][] A, int[][] B) {
        int rowsA = A.length;
        int colsA = A[0].length;
        int colsB = B[0].length;
        int[][] result = new int[rowsA][colsB];
        
        for (int i = 0; i < rowsA; i++) {
            for (int j = 0; j < colsB; j++) {
                int sum = 0;
                for (int k = 0; k < colsA; k++) {
                    sum += A[i][k] * B[k][j];
                }
                result[i][j] = sum;
            }
        }
        return result;
    }
    
    private int[][] addMatrixBlocks(int[][] A, int[][] B) {
        int rows = A.length;
        int cols = A[0].length;
        int[][] result = new int[rows][cols];
        
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                result[i][j] = A[i][j] + B[i][j];
            }
        }
        return result;
    }
    
    private int[][] transposeMatrix(int[][] matrix) {
        int rows = matrix.length;
        int cols = matrix[0].length;
        int[][] result = new int[cols][rows];
        
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                result[j][i] = matrix[i][j];
            }
        }
        return result;
    }
    
    private void startHeartbeat() {
        if (heartbeatExecutor != null) {
            heartbeatExecutor.scheduleAtFixedRate(() -> {
                if (running.get() && connected.get()) {
                    try {
                        sendMessage(Message.createHeartbeat(workerId));
                    } catch (Exception e) {
                        // Silently fail
                    }
                }
            }, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        }
    }
    
    private synchronized void sendMessage(Message msg) {
        if (!connected.get() || out == null) return;
        
        try {
            byte[] data = msg.pack();
            out.writeInt(data.length);
            out.write(data);
            out.flush();
        } catch (IOException e) {
            connected.set(false);
        }
    }
    
    private Message receiveMessage() throws IOException {
        int length = in.readInt();
        byte[] data = new byte[length];
        in.readFully(data);
        return Message.unpack(data);
    }
    
    private void closeConnection() {
        connected.set(false);
        try {
            if (masterSocket != null && !masterSocket.isClosed()) {
                masterSocket.close();
            }
        } catch (IOException e) {
            // Ignore
        }
    }
    
    public void shutdown() {
        running.set(false);
        connected.set(false);
        
        if (taskExecutor != null) taskExecutor.shutdown();
        if (communicationExecutor != null) communicationExecutor.shutdown();
        if (heartbeatExecutor != null) heartbeatExecutor.shutdown();
        
        closeConnection();
    }
    
    public boolean isConnected() {
        return connected.get();
    }
    
    public int getTasksCompleted() {
        return tasksCompleted.get();
    }
    
    /**
     * RPC-style remote invocation support
     * Allows calling worker methods remotely
     */
    public Object invokeRPC(String method, Object... args) {
        // RPC abstraction for remote method invocation
        switch (method) {
            case "getTasksCompleted":
                return getTasksCompleted();
            case "isConnected":
                return isConnected();
            case "getWorkerId":
                return workerId;
            default:
                throw new UnsupportedOperationException("RPC method not supported: " + method);
        }
    }
    
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: java Worker <workerId> <masterHost> <masterPort>");
            System.exit(1);
        }
        
        Worker worker = new Worker(args[0]);
        worker.joinCluster(args[1], Integer.parseInt(args[2]));
        
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            worker.shutdown();
        }
    }
}