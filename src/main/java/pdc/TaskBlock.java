package pdc;

import java.io.*;


public class TaskBlock implements Serializable {
    private static final long serialVersionUID = 1L;
    
    // Task identification
    int taskId;
    String operation;
    
    // Matrix data
    int[][] matrixA;
    int[][] matrixB;
    int[][] result;
    
    // Execution tracking
    String assignedWorker;
    long receiveTime;
    long completionTime;
    TaskStatus status;
    String error;
    
    // Metadata for block position in larger matrix
    int blockRow;
    int blockCol;
    
    /**
     * Constructor for creating a new task
     */
    public TaskBlock(int taskId, String operation) {
        this.taskId = taskId;
        this.operation = operation;
        this.status = TaskStatus.PENDING;
    }
    
    /**
     * Serialize task to byte array for network transmission
     */
    public byte[] serialize() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(this);
            oos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize task", e);
        }
    }
    
    /**
     * Deserialize task from byte array
     */
    public static TaskBlock deserialize(byte[] data) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            ObjectInputStream ois = new ObjectInputStream(bais);
            return (TaskBlock) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to deserialize task", e);
        }
    }
    
    @Override
    public String toString() {
        return String.format("TaskBlock{id=%d, operation=%s, status=%s, worker=%s}", 
                taskId, operation, status, assignedWorker);
    }
}