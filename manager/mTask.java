package manager;

public class mTask {
    private long processing_id = 0;
    private String data;
    private long start = 0;
    private long end;
    private boolean started = false;
    private boolean running = false;
    
    public mTask( long proc_id_in, String input ) {
        processing_id = proc_id_in;
        data = input;
    }
    
    public void start() {
        start = System.currentTimeMillis() / 1000L;
        started = true;
        running = true;
    }
    
    public void finish() {
        end = System.currentTimeMillis() / 1000L;
        running = false;
    }
    
    public long get_finish_time() {
        if( !started ) return -1;
        if( running ) return -1;
        return end;
    }
    
    public int status() {
        if( !started ) return 0; // waiting to start
        if( running ) return 1; // running
        return 2; // finished
    }
    
    public boolean isdone() {
        if( running ) return false;
        return true;
    }
    
    public long time() {
        if( !started ) return -1;
        if( running ) {
            end = System.currentTimeMillis() / 1000L;
        }
        long dif = end - start;
        return dif;
    }
    
    public String toString() {
        int status = status();
        long time = time();
        String str = "Command = " + data;
        if( status != 0 ) {
            str = str + ", Time = " + Long.toString( time );
        }
        return str;
    }
}