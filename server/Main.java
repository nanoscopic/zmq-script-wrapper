package server;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.io.IOException;
import org.zeromq.ZMQ;

class ProcessWatcher implements Runnable {
    private Process proc;
    boolean dead = false;
    public void init( Process procin ) {
        this.proc = procin;
    }
    public void run() {
        try { proc.waitFor(); } catch( java.lang.InterruptedException ie ) {}
        synchronized( this ) {
            dead = true;
        }
    }
    public boolean isdead() {
        boolean cur;
        synchronized( this ) {
            cur = dead;
        }
        return cur;
    }
}

public class Main {
    static ZMQ.Socket socket_to_man             = null;
    static ZMQ.Socket socket_from_man           = null;
    static ZMQ.Socket socket_from_man_broadcast = null;
    static long processing_id = 0;
    static long server_id = 0;
    static BufferedReader br = null;
    static OutputStreamWriter osr = null;
    static int stop = 0;
    static Process sub = null;
    
    static long port_1 = 5555;
    static long port_2 = 6666;
    static long port_3 = 6667;
    static long min_servers = 3;
    static long max_servers = 4;
    // After this many seconds of inactivity, servers beyond min_servers will be shut down
    static long server_inactive_timeout_seconds = 1000;
    static String looping_cmd = "perl test.pl";
    static ProcessWatcher pw = null;
    static Thread curwatcher = null;
    
    private static boolean read_conf( String conf_filename ) {
        JSONParser parser = new JSONParser();
        JSONObject root = null;
        
        try {
            root = (JSONObject) parser.parse( new java.io.FileReader( conf_filename ) );
        }
        catch( java.io.FileNotFoundException e ) { e.printStackTrace(); }
        catch( IOException           e ) { e.printStackTrace(); }
        catch( ParseException e        ) { e.printStackTrace(); }
        
        if( root == null ) { return false; }
        if( root.containsKey( "port_1"      ) ) port_1      = (long)   root.get("port_1");
        if( root.containsKey( "port_2"      ) ) port_2      = (long)   root.get("port_2");
        if( root.containsKey( "port_3"      ) ) port_3      = (long)   root.get("port_3");
        if( root.containsKey( "min_servers" ) ) min_servers = (long)   root.get("min_servers");
        if( root.containsKey( "max_servers" ) ) max_servers = (long)   root.get("max_servers");
        if( root.containsKey( "looping_cmd" ) ) looping_cmd = (String) root.get("looping_cmd");
        if( root.containsKey( "server_inactive_timeout_seconds" ) )
            server_inactive_timeout_seconds = (long) root.get("server_inactive_timeout_seconds");
        return true;
    }
    
    public static void main(String[] args) throws IOException, InterruptedException {
        if( args.length == 0 ) {
            System.out.println("You need to specify a configuration file" );
            return;
        }
        String conf_filename = args[0];
        if( !read_conf( conf_filename ) ) { return; }
        
        ZMQ.Context context = ZMQ.context(1);
        //ZMQ.Context c2 = ZMQ.context(1);
        //context.setIOThreads(4);
        
        socket_from_man  = context.socket( ZMQ.PULL );
        socket_from_man.connect("tcp://localhost:" + Long.toString( port_2 ) ); //6666
        if( socket_from_man == null ) {
            System.out.println("Could not connect tcp localhost 6666");
            return;
        }
        
        socket_from_man_broadcast = context.socket( ZMQ.SUB ); // subscribe to broadcast messages
        socket_from_man_broadcast.connect("tcp://localhost:" + Long.toString( port_3 ) ); //6667
        if( socket_from_man_broadcast == null ) {
            System.out.println("Could not connect tcp localhost 6667");
            return;
        }
        // subscribe to the main broadcast channel; we will subscribe to an individual channel later after registration
        socket_from_man_broadcast.subscribe( "ALL".getBytes(ZMQ.CHARSET) );
        
        socket_to_man = context.socket( ZMQ.REQ );
        socket_to_man.connect( "tcp://localhost:" + Long.toString( port_1 ) ); //5555
        if( socket_to_man == null ) {
            System.out.println("Could not connect tcp localhost 5555");
            return;
        }
        
        //socket_to_man.setBacklog( 15 );
        
        //dummy_manager_request();
        server_id = get_server_id();
        
        // Subscribe to messages send directly to us
        String serverSub = "server-" + Long.toString( server_id );
        socket_from_man_broadcast.subscribe( serverSub.getBytes( ZMQ.CHARSET ) );
        
        System.out.println( "Started server; assigned id from manager: " + Long.toString( server_id ) );
        
        boolean ok = start_process();
        if( !ok ) { System.exit( 1 ); }
        
        System.out.println("Start of loop\n");
        while( stop==0 ) { //&& !Thread.currentThread().isInterrupted() ) {
            // get a request from the manager
            //System.out.println("Received " + ": [" + new String( reply, ZMQ.CHARSET ) + "]" );
            //System.out.println("Start of loop\n");
            
            //while( true ) {
                //System.out.print(".");
                byte[] json_bytes = socket_from_man.recv( ZMQ.DONTWAIT );
                if( json_bytes != null ) {
                    String json_str = new String( json_bytes, ZMQ.CHARSET );
                    handle_generic_msg( json_str );
                    //break;
                }
                else {
                    String dest = socket_from_man_broadcast.recvStr( ZMQ.DONTWAIT );
                    if( dest != null ) {
                        byte[] jb2 = socket_from_man_broadcast.recv( 0 );
                        if( jb2 != null ) {
                            String json_str = new String( jb2, ZMQ.CHARSET );
                            System.out.println("Got " + json_str );
                            handle_broadcast_msg( json_str );
                            //break;
                        }
                    }
                }
                
                if( stop == 0 && pw.isdead() ) {
                    boolean ok2 = start_process();
                    if( !ok2 ) { System.exit( 1 ); }
                }
                
                Thread.sleep( 100 );
                //System.out.println("Looping: stop=" + Integer.toString( stop ) );
                //sleep a bit
            //}
        }
        //if( curwatcher != null ) { curwatcher.stop(); }
        
        socket_from_man.close();
        socket_to_man.close();
        socket_from_man_broadcast.close();
        context.term();
        System.exit( 0 );
        //c2.term();
    }
    
    private static boolean start_process() {
        send_update("{\"type\":\"process_started\"}");
        try {
            Runtime RT = Runtime.getRuntime();
            
            System.out.println("Running: " + looping_cmd );
            sub = RT.exec( looping_cmd );
            
            pw = new ProcessWatcher();
            pw.init( sub );
            if( curwatcher != null ) curwatcher.stop();
            curwatcher = new Thread( pw );
            curwatcher.start();
            
            InputStreamReader isr = new InputStreamReader( sub.getInputStream() );
            br = new BufferedReader( isr );
            osr = new OutputStreamWriter( sub.getOutputStream() );
            System.out.println("Started and connected to subprocess");
            return true;
        }
        catch( Exception e ) {
            System.out.println("Could not start subprocess");
            return false;
        }
    }
    
    public static void handle_broadcast_msg( String json_str ) throws IOException  {
        // check to ensure this is a message we should pay attention to
        JSONObject json = (JSONObject) JSONValue.parse( json_str );
        String type = (String) json.get("type");
        
        if( type.equals("stop") ) {
            System.out.println("Server " + Long.toString( server_id ) + " quitting" );
            
            /*try {
                String filename= "test.txt";
                java.io.FileWriter fw = new java.io.FileWriter(filename,true); //the true will append the new data
                fw.write("Server " + Long.toString( server_id ) + "quitting\n");//appends the string to the file
                fw.close();
            }
            catch(IOException ioe) { System.err.println("IOException: " + ioe.getMessage()); }*/
            
            if( !pw.isdead() ) {
                String command = "stop\n";
                osr.write( command );
                osr.flush();
                try{ sub.waitFor(); } catch( Exception e ) {} // InterruptedException
            }
            
            send_update("{\"type\":\"goodbye\"}");
            stop = 1;
        }
        else {
            System.out.println("Received type " + type );
        }
    }
    
    public static void handle_generic_msg( String json_str ) throws IOException  {
        JSONObject json = (JSONObject) JSONValue.parse( json_str );
        String type = (String) json.get("type");
        
        if( type.equals("start_report" ) ) {
            processing_id = (long) json.get("processing_id");
            String start_res = 
                "{\"type\":\"task_start\",\"server_id\":"     + Long.toString( server_id     ) + 
                ",\"processing_id\":" + Long.toString( processing_id ) + "}";
            send_update( start_res );
            
            if( pw.isdead() ) {
                boolean ok = start_process();
                if( !ok ) { System.exit( 1 ); }
            }
            
            String command = (String) json.get("command");
            if( command == null ) { command = "none"; }
            command += "\n";
            osr.write( command );
            osr.flush();
            while( true ) {
                String line = br.readLine();
                if( line.equals("--DONE--" ) ) {
                    System.out.println("Got --DONE-- from subprocess; sending finish\n");
                    JSONObject ob = new JSONObject();
                    ob.put("type", "report_done" );
                    ob.put("server_id", server_id );
                    ob.put("processing_id", processing_id );
                    String json2 = ob.toString();
                    
                    System.out.println("Sending: " + json2 + "\n" );
                    send_update( json2 );
                    break;
                }
                else {
                    System.out.println("Got a line: " + line + "\n" );
                    JSONObject obj = new JSONObject();
                    obj.put("type", "report_update" );
                    obj.put("line", line );
                    obj.put("server_id", server_id );
                    obj.put("processing_id", processing_id );
                    String json2 = obj.toString();
                    System.out.println("Sending: " + json2 + "\n" );
                    send_update( json2 );
                }
            }
        }
        else if( type.equals("stop") ) {
            /*try {
                String filename= "/opt/bfms_trunk/birt_reporting/zmq_script_wrapper/test.txt";
                java.io.FileWriter fw = new java.io.FileWriter(filename,true); //the true will append the new data
                fw.write("Bad Server " + Long.toString( server_id ) + "quitting\n");//appends the string to the file
                fw.close();
            }
            catch(IOException ioe) { System.err.println("IOException: " + ioe.getMessage()); }*/
            
            String command = "stop\n";
            osr.write( command );
            stop = 1;
        }
        else {
            String request = "{\"type\":\"error\"}";
            
            //socket.send( request.getBytes( ZMQ.CHARSET ), 0 );
        }
    }
    
    public static void dummy_manager_request() { 
        String request = "{\"type\":\"dummy\"}";
        
        //socket_to_man.connect( "tcp://localhost:5555" );
        
        
        socket_to_man.send( request.getBytes( ZMQ.CHARSET ), 0 );
        
        //socket_to_man.disconnect();
    }
    
    public static long get_server_id() {
        //socket_to_man.connect( "tcp://localhost:5555" );
        
        
        String msg = "{\"type\":\"assign_server_id\"}";
        socket_to_man.send( msg.getBytes( ZMQ.CHARSET ), 0 );
        byte[] json_bytes = socket_to_man.recv( 0 );
        //socket_to_man.close();
        
        System.out.println( "Requesting server id from manager" );
        
        String json_str = new String( json_bytes, ZMQ.CHARSET );
        JSONObject json = (JSONObject) JSONValue.parse( json_str );
        System.out.println("Received: " + json_str );
        
        long server_id = (long) json.get("server_id");
        //long server_id = 0;
        
        return server_id;
        //return Integer.parseInt( server_id );
    }
    
    public static void send_update( String line ) {
        //while( true ) {
        //    try {
                //socket_to_man.connect( "tcp://localhost:5555" );
        //        break;
        //    }
        //    catch( Exception e ) {
        //    }
        //}
        socket_to_man.send( line.getBytes( ZMQ.CHARSET ), 0 );
        byte[] reply = socket_to_man.recv(0);
        //socket_to_man.close();
        
        System.out.println( "Sending request" );
        
        System.out.println("Received " + new String( reply, ZMQ.CHARSET ) );
        
    }
}