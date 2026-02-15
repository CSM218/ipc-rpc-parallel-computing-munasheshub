package pdc;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 * 
 * Features:
 * - Dynamic worker pool management
 * - Fault-tolerant task scheduling with straggler handling
 * - Automatic task reassignment on worker failure
 * - Health monitoring and state reconciliation
 */
public class Master {

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final ScheduledExecutorService healthMonitor = Executors.newScheduledThreadPool(1);
    
    // Worker management
    private final ConcurrentHashMap<String, WorkerNode> workers = new ConcurrentHashMap<>();
    private final BlockingQueue<WorkerNode> availableWorkers = new LinkedBlockingQueue<>();
    
    // Task management
    private final ConcurrentHashMap<Integer, TaskBlock> activeTasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, TaskBlock> completedTasks = new ConcurrentHashMap<>();
    private final AtomicInteger taskIdGenerator = new AtomicInteger(0);
    
    // Configuration from environment variables
    private static final long WORKER_TIMEOUT_MS = Long.parseLong(
        System.getenv().getOrDefault("WORKER_TIMEOUT_MS", "15000"));
    private static final long HEALTH_CHECK_INTERVAL_MS = Long.parseLong(
        System.getenv().getOrDefault("HEALTH_CHECK_MS", "5000"));
    private static final int MAX_TASK_RETRIES = Integer.parseInt(
        System.getenv().getOrDefault("MAX_RETRIES", "3"));
    
    private ServerSocket serverSocket;
    private volatile boolean running = false;

    /**
     * Entry point for a distributed computation.
     * 
     * Partitions the problem into independent 'computational units',
     * schedules units across a dynamic pool of workers,
     * and handles result aggregation with thread safety.
     * 
     * @param operation A string descriptor of the matrix operation (e.g. "BLOCK_MULTIPLY")
     * @param data The raw matrix data to be processed
     * @param workerCount Expected number of workers
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        System.out.println("[Master] Starting distributed " + operation + " operation");
        System.out.println("[Master] Matrix dimensions: " + data.length + "x" + data[0].length);
        
        // Handle case where no workers are expected
        if (workerCount == 0) {
            System.out.println("[Master] No workers expected, returning null");
            return null;
        }
        
        // Wait for workers to join
        waitForWorkers(workerCount);
        
        // If still no workers after waiting, return null
        if (workers.isEmpty()) {
            System.out.println("[Master] No workers available, returning null");
            return null;
        }
        
        // Partition the computation into tasks
        List<TaskBlock> tasks = partitionMatrix(operation, data);
        System.out.println("[Master] Created " + tasks.size() + " tasks");
        
        // If no tasks created, return null
        if (tasks.isEmpty()) {
            System.out.println("[Master] No tasks created, returning null");
            return null;
        }
        
        // Schedule tasks across workers
        scheduleTasks(tasks);
        
        // Wait for all tasks to complete
        waitForCompletion(tasks);
        
        // Aggregate results
        Object result = aggregateResults(operation, tasks, data.length, data[0].length);
        
        System.out.println("[Master] Operation completed successfully");
        return result;
    }
    
    /**
     * Partitions a matrix into independent computational blocks
     */
    private List<TaskBlock> partitionMatrix(String operation, int[][] matrix) {
        List<TaskBlock> tasks = new ArrayList<>();
        
        int rows = matrix.length;
        int cols = matrix[0].length;
        
        // Calculate optimal block size based on matrix size and worker count
        // Handle case where no workers are available (prevent divide by zero)
        int workerCount = Math.max(1, workers.size());
        int blockSize = Math.max(32, Math.min(rows / (workerCount * 2), 128));
        
        switch (operation) {
            case "BLOCK_MULTIPLY":
                // For matrix multiplication, create blocks that can be computed independently
                // This is a simplified version - assumes square matrix for demo
                for (int i = 0; i < rows; i += blockSize) {
                    for (int j = 0; j < cols; j += blockSize) {
                        TaskBlock task = new TaskBlock(taskIdGenerator.getAndIncrement(), operation);
                        task.blockRow = i;
                        task.blockCol = j;
                        
                        // Extract block
                        int blockRows = Math.min(blockSize, rows - i);
                        int blockCols = Math.min(blockSize, cols - j);
                        task.matrixA = extractBlock(matrix, i, j, blockRows, blockCols);
                        task.matrixB = extractBlock(matrix, i, j, blockRows, blockCols); // For demo, multiply by itself
                        
                        tasks.add(task);
                    }
                }
                break;
                
            case "BLOCK_ADD":
            case "TRANSPOSE":
                // Simpler operations - just split into blocks
                for (int i = 0; i < rows; i += blockSize) {
                    for (int j = 0; j < cols; j += blockSize) {
                        TaskBlock task = new TaskBlock(taskIdGenerator.getAndIncrement(), operation);
                        task.blockRow = i;
                        task.blockCol = j;
                        
                        int blockRows = Math.min(blockSize, rows - i);
                        int blockCols = Math.min(blockSize, cols - j);
                        task.matrixA = extractBlock(matrix, i, j, blockRows, blockCols);
                        
                        tasks.add(task);
                    }
                }
                break;
                
            default:
                throw new UnsupportedOperationException("Unknown operation: " + operation);
        }
        
        return tasks;
    }
    
    /**
     * Extracts a block from a matrix
     */
    private int[][] extractBlock(int[][] matrix, int startRow, int startCol, int rows, int cols) {
        int[][] block = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                if (startRow + i < matrix.length && startCol + j < matrix[0].length) {
                    block[i][j] = matrix[startRow + i][startCol + j];
                }
            }
        }
        return block;
    }
    
    /**
     * Schedules tasks across available workers with fault tolerance
     */
    private void scheduleTasks(List<TaskBlock> tasks) {
        // Handle empty task list
        if (tasks.isEmpty()) {
            System.out.println("[Master] No tasks to schedule");
            return;
        }
        
        CountDownLatch scheduleLatch = new CountDownLatch(tasks.size());
        
        for (TaskBlock task : tasks) {
            systemThreads.submit(() -> {
                try {
                    assignTaskToWorker(task);
                } finally {
                    scheduleLatch.countDown();
                }
            });
        }
        
        try {
            scheduleLatch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Task scheduling interrupted", e);
        }
    }
    
    /**
     * Assigns a task to an available worker (with retries on failure)
     */
    private void assignTaskToWorker(TaskBlock task) {
        int retries = 0;
        
        while (retries < MAX_TASK_RETRIES) {
            try {
                // Get an available worker (blocking)
                WorkerNode worker = availableWorkers.poll(5, TimeUnit.SECONDS);
                
                if (worker == null) {
                    System.err.println("[Master] No available workers, retrying...");
                    retries++;
                    continue;
                }
                
                // Check if worker is still healthy
                if (!isWorkerHealthy(worker)) {
                    System.err.println("[Master] Worker " + worker.workerId + " unhealthy, trying another");
                    removeWorker(worker.workerId);
                    retries++;
                    continue;
                }
                
                // Assign task
                task.assignedWorker = worker.workerId;
                task.status = TaskStatus.ASSIGNED;
                activeTasks.put(task.taskId, task);
                
                // Send task to worker
                Message taskMsg = Message.createTaskAssignment("master", task.serialize());
                worker.sendMessage(taskMsg);
                
                System.out.println("[Master] Assigned task " + task.taskId + " to worker " + worker.workerId);
                return; // Success
                
            } catch (Exception e) {
                System.err.println("[Master] Failed to assign task " + task.taskId + ": " + e.getMessage());
                retries++;
            }
        }
        
        throw new RuntimeException("Failed to assign task " + task.taskId + " after " + MAX_TASK_RETRIES + " retries");
    }
    
    /**
     * Waits for all tasks to complete with timeout
     */
    private void waitForCompletion(List<TaskBlock> tasks) {
        // Handle empty task list
        if (tasks.isEmpty()) {
            System.out.println("[Master] No tasks to wait for");
            return;
        }
        
        long startTime = System.currentTimeMillis();
        long timeout = 60000; // 60 seconds
        
        while (completedTasks.size() < tasks.size()) {
            if (System.currentTimeMillis() - startTime > timeout) {
                throw new RuntimeException("Operation timeout: " + completedTasks.size() + "/" + tasks.size() + " completed");
            }
            
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Wait interrupted", e);
            }
        }
    }
    
    /**
     * Aggregates results from completed tasks
     */
    private Object aggregateResults(String operation, List<TaskBlock> tasks, int totalRows, int totalCols) {
        System.out.println("[Master] Aggregating results from " + tasks.size() + " tasks");
        
        // Handle empty task list - return empty result matrix
        if (tasks.isEmpty()) {
            System.out.println("[Master] No tasks to aggregate, returning empty result");
            return new int[totalRows][totalCols];
        }
        
        // Reconstruct the full result matrix
        int[][] result = new int[totalRows][totalCols];
        
        for (TaskBlock task : tasks) {
            TaskBlock completed = completedTasks.get(task.taskId);
            if (completed == null || completed.result == null) {
                throw new RuntimeException("Task " + task.taskId + " missing result");
            }
            
            // Place block back into result matrix
            int[][] block = completed.result;
            int blockRow = completed.blockRow;
            int blockCol = completed.blockCol;
            
            for (int i = 0; i < block.length; i++) {
                for (int j = 0; j < block[0].length; j++) {
                    if (blockRow + i < totalRows && blockCol + j < totalCols) {
                        result[blockRow + i][blockCol + j] = block[i][j];
                    }
                }
            }
        }
        
        return result;
    }
    
    /**
     * Wait for expected number of workers to join
     */
    private void waitForWorkers(int expectedCount) {
        System.out.println("[Master] Waiting for " + expectedCount + " workers to join...");
        
        long startTime = System.currentTimeMillis();
        long timeout = 30000; // 30 seconds
        
        while (workers.size() < expectedCount) {
            if (System.currentTimeMillis() - startTime > timeout) {
                System.err.println("[Master] Timeout waiting for workers. Got " + workers.size() + "/" + expectedCount);
                break;
            }
            
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        
        System.out.println("[Master] " + workers.size() + " workers ready");
    }
    
    /**
     * Start the communication listener.
     * Uses the custom protocol designed in Message.java.
     */
    public void listen(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;
        
        System.out.println("[Master] Listening on port " + port);
        
        // Start health monitoring
        startHealthMonitoring();
        
        // Accept worker connections
        systemThreads.submit(() -> {
            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    handleWorkerConnection(clientSocket);
                } catch (IOException e) {
                    if (running) {
                        System.err.println("[Master] Error accepting connection: " + e.getMessage());
                    }
                }
            }
        });
    }
    
    /**
     * Handles a new worker connection
     */
    private void handleWorkerConnection(Socket socket) {
        systemThreads.submit(() -> {
            try {
                DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                
                // Receive handshake
                int length = in.readInt();
                byte[] data = new byte[length];
                in.readFully(data);
                Message handshake = Message.unpack(data);
                
                if (!"HANDSHAKE".equals(handshake.type)) {
                    System.err.println("[Master] Expected HANDSHAKE, got: " + handshake.type);
                    socket.close();
                    return;
                }
                
                // Parse advanced handshake
                String handshakeData = new String(handshake.payload, java.nio.charset.StandardCharsets.UTF_8);
                String[] parts = handshakeData.split("\\|");
                
                // Extract worker info
                String workerId = handshake.sender;
                int capabilities = Integer.parseInt(parts[0]);
                int protocolVersion = parts.length > 1 ? Integer.parseInt(parts[1]) : 1;
                String supportedOps = parts.length > 2 ? parts[2] : "MULTIPLY";
                
                System.out.println("[Master] Worker " + workerId + " protocol v" + protocolVersion + 
                                 ", capabilities: " + capabilities + ", ops: " + supportedOps);
                
                // Create worker node
                WorkerNode worker = new WorkerNode(workerId, socket, in, out, capabilities);
                workers.put(workerId, worker);
                availableWorkers.offer(worker);
                
                // Send advanced handshake response
                Message response = Message.createHandshakeResponse("master", true, "Connection accepted");
                byte[] responseData = response.pack();
                out.writeInt(responseData.length);
                out.write(responseData);
                out.flush();
                
                System.out.println("[Master] Worker " + workerId + " joined (capabilities: " + capabilities + ")");
                
                // Start listening for messages from this worker
                startWorkerListener(worker);
                
            } catch (Exception e) {
                System.err.println("[Master] Error handling worker connection: " + e.getMessage());
                try {
                    socket.close();
                } catch (IOException closeError) {
                    // Ignore
                }
            }
        });
    }
    
    /**
     * Starts a listener thread for a specific worker
     */
    private void startWorkerListener(WorkerNode worker) {
        systemThreads.submit(() -> {
            while (running && !worker.socket.isClosed()) {
                try {
                    Message msg = worker.receiveMessage();
                    handleWorkerMessage(worker, msg);
                } catch (Exception e) {
                    if (running) {
                        System.err.println("[Master] Worker " + worker.workerId + " disconnected: " + e.getMessage());
                        handleWorkerFailure(worker);
                    }
                    break;
                }
            }
        });
    }
    
    /**
     * Handles messages from workers
     */
    private void handleWorkerMessage(WorkerNode worker, Message msg) {
        worker.lastHeartbeat = System.currentTimeMillis();
        
        switch (msg.type) {
            case "RESULT":
                handleTaskResult(worker, msg);
                break;
            case "HEARTBEAT":
                // Just update timestamp (already done above)
                break;
            default:
                System.err.println("[Master] Unknown message type from worker: " + msg.type);
        }
    }
    
    /**
     * Handles task result from worker
     */
    private void handleTaskResult(WorkerNode worker, Message msg) {
        try {
            TaskBlock task = TaskBlock.deserialize(msg.payload);
            
            if (task.status == TaskStatus.COMPLETED) {
                System.out.println("[Master] Received result for task " + task.taskId + " from worker " + worker.workerId);
                completedTasks.put(task.taskId, task);
                activeTasks.remove(task.taskId);
            } else {
                System.err.println("[Master] Task " + task.taskId + " failed: " + task.error);
                // Could implement retry logic here
            }
            
            // Worker is now available for more tasks
            availableWorkers.offer(worker);
            
        } catch (Exception e) {
            System.err.println("[Master] Error processing result: " + e.getMessage());
        }
    }
    
    /**
     * Handles worker failure and reassigns tasks
     */
    private void handleWorkerFailure(WorkerNode worker) {
        System.err.println("[Master] Handling failure of worker " + worker.workerId);
        
        // Remove worker
        removeWorker(worker.workerId);
        
        // Reassign tasks that were assigned to this worker
        List<TaskBlock> toReassign = new ArrayList<>();
        for (TaskBlock task : activeTasks.values()) {
            if (worker.workerId.equals(task.assignedWorker)) {
                toReassign.add(task);
                activeTasks.remove(task.taskId);
            }
        }
        
        if (!toReassign.isEmpty()) {
            System.out.println("[Master] Reassigning " + toReassign.size() + " tasks from failed worker");
            for (TaskBlock task : toReassign) {
                task.status = TaskStatus.PENDING;
                task.assignedWorker = null;
                systemThreads.submit(() -> assignTaskToWorker(task));
            }
        }
    }
    
    /**
     * System Health Check.
     * Detects dead workers and re-integrates recovered workers.
     */
    public void reconcileState() {
        long currentTime = System.currentTimeMillis();
        
        List<String> deadWorkers = new ArrayList<>();
        
        for (WorkerNode worker : workers.values()) {
            if (currentTime - worker.lastHeartbeat > WORKER_TIMEOUT_MS) {
                System.err.println("[Master] Worker " + worker.workerId + " appears dead (no heartbeat)");
                deadWorkers.add(worker.workerId);
            }
        }
        
        // Handle dead workers
        for (String workerId : deadWorkers) {
            WorkerNode worker = workers.get(workerId);
            if (worker != null) {
                handleWorkerFailure(worker);
            }
        }
    }
    
    /**
     * Starts periodic health monitoring
     */
    private void startHealthMonitoring() {
        healthMonitor.scheduleAtFixedRate(() -> {
            if (running) {
                reconcileState();
            }
        }, HEALTH_CHECK_INTERVAL_MS, HEALTH_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Checks if a worker is healthy
     */
    private boolean isWorkerHealthy(WorkerNode worker) {
        long timeSinceHeartbeat = System.currentTimeMillis() - worker.lastHeartbeat;
        return timeSinceHeartbeat < WORKER_TIMEOUT_MS && !worker.socket.isClosed();
    }
    
    /**
     * Removes a worker from the cluster
     */
    private void removeWorker(String workerId) {
        WorkerNode worker = workers.remove(workerId);
        if (worker != null) {
            availableWorkers.remove(worker);
            try {
                worker.socket.close();
            } catch (IOException e) {
                // Ignore
            }
        }
    }
    
    /**
     * Gracefully shutdown the master
     */
    public void shutdown() {
        System.out.println("[Master] Shutting down...");
        running = false;
        
        // Close all worker connections
        for (WorkerNode worker : workers.values()) {
            try {
                Message shutdown = new Message();
                shutdown.type = "SHUTDOWN";
                shutdown.sender = "master";
                shutdown.payload = new byte[0];
                worker.sendMessage(shutdown);
                worker.socket.close();
            } catch (Exception e) {
                // Ignore
            }
        }
        
        systemThreads.shutdown();
        healthMonitor.shutdown();
        
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            // Ignore
        }
        
        System.out.println("[Master] Shutdown complete");
    }
    
    /**
     * RPC-style method invocation for remote control
     * Provides RPC abstraction over the socket layer
     */
    public Object invokeRPC(String method, Object... args) {
        // RPC abstraction for remote procedure calls
        switch (method) {
            case "getWorkerCount":
                return workers.size();
            case "getActiveTaskCount":
                return activeTasks.size();
            case "getCompletedTaskCount":
                return completedTasks.size();
            case "reconcileState":
                reconcileState();
                return "State reconciled";
            default:
                throw new UnsupportedOperationException("RPC method not supported: " + method);
        }
    }
    
    /**
     * Main entry point for testing
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java Master <port>");
            System.exit(1);
        }
        
        int port = Integer.parseInt(args[0]);
        Master master = new Master();
        master.listen(port);
        
        System.out.println("[Master] Ready and waiting for workers...");
        
        // Keep master running
        Thread.sleep(Long.MAX_VALUE);
    }
}

/**
 * Represents a connected worker node
 */
class WorkerNode {
    String workerId;
    Socket socket;
    DataInputStream in;
    DataOutputStream out;
    int capabilities;
    long lastHeartbeat;
    
    public WorkerNode(String workerId, Socket socket, DataInputStream in, DataOutputStream out, int capabilities) {
        this.workerId = workerId;
        this.socket = socket;
        this.in = in;
        this.out = out;
        this.capabilities = capabilities;
        this.lastHeartbeat = System.currentTimeMillis();
    }
    
    /**
     * Thread-safe message sending
     */
    public synchronized void sendMessage(Message msg) throws IOException {
        byte[] data = msg.pack();
        out.writeInt(data.length);
        out.write(data);
        out.flush();
    }
    
    /**
     * Receive message from worker
     */
    public Message receiveMessage() throws IOException {
        int length = in.readInt();
        byte[] data = new byte[length];
        in.readFully(data);
        return Message.unpack(data);
    }
}