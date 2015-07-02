package manager;
import org.zeromq.ZMQ;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.io.IOException;

import java.sql.Array;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.HashMap;
import sun.misc.SignalHandler;
import sun.misc.Signal;
import com.codechild.SystemDTools;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import manager.mTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

class MySignalHandler implements SignalHandler {
    private SignalHandler oldHandler;
    static ZMQ.Context context;
    static String conf_file;
    
    public static MySignalHandler install( String signalName, ZMQ.Context cin, String conf_file_in ) {
        Signal mySignal = new Signal( signalName );
        MySignalHandler myHandler = new MySignalHandler();
        myHandler.oldHandler = Signal.handle( mySignal, myHandler );
        context = cin;
        conf_file = conf_file_in;
        return myHandler;
    }
    
    public void handle( Signal sig ) {
        System.out.println("Diagnostic Signal handler called for signal "+sig);
        
        Thread x = new Thread() {
            @Override
            public void run() {
                /*try{ Thread.sleep( 100 ); } catch( Exception e ) {}
                //ZMQ.Context ct2 = ZMQ.context( 1 );
                ZMQ.Socket socket_to_man = context.socket( ZMQ.REQ ); // send a message to ourselves
                socket_to_man.connect( "tcp://localhost:5555" );
                String line = "{\"type\":\"shutdown\"}";
                socket_to_man.send( line.getBytes( ZMQ.CHARSET ), 0 );
                //byte[] reply = socket_to_man.recv(0);
                socket_to_man.close();
                //ct2.term();*/
                SystemDTools x = new SystemDTools();
                x.sd_notify( 0, "STOPPING=1" );
                
                Runtime rt = Runtime.getRuntime();
                try { rt.exec("java -jar client.jar " + conf_file + " shutdown"); } catch( java.io.IOException e ) {}
                System.out.println("Sent shutdown command to myself");
            }
        };
        x.start();
        
        //if ( oldHandler != SIG_DFL && oldHandler != SIG_IGN ) {
        //    oldHandler.handle(sig);
        //}
    }
}


class StreamGobbler extends Thread {
    InputStream is;
    String type;

    public StreamGobbler( InputStream is, String type) {
        this.is = is;
        this.type = type;
    }

    @Override
    public void run() {
        try {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            while ((line = br.readLine()) != null)
                System.out.println(type + "> " + line);
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}

public class Main {
    static ZMQ.Socket socket_toserver;
    static long next_server_id = 1;
    static Map<String,ArrayDeque<String>> quemap = null;
    static Map<String,Integer> donemap = null;
    static Map<Long,mTask> taskmap = null;
    static long next_processing_id = 1;
    static long server_count = 0;
    
    static long port_1 = 5555;
    static long port_2 = 6666;
    static long port_3 = 6667;
    static long min_servers = 3;
    static long max_servers = 4;
    // After this many seconds of inactivity, servers beyond min_servers will be shut down
    static long server_inactive_timeout_seconds = 1000;
    static String looping_cmd = "perl test.pl";
    static SystemDTools sdt = new SystemDTools();
    static int num_tasks = 0;
    
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
    
    public static void main(String[] args) throws Exception {
        boolean response_delayed = false;
        
        if( args.length == 0 ) {
            System.out.println("You need to specify a configuration file" );
            return;
        }
        String conf_filename = args[0];
        if( !read_conf( conf_filename ) ) { return; }
        
        ZMQ.Context context = ZMQ.context(1);
        
        MySignalHandler.install( "INT", context, conf_filename );
        
        // This socket is for responding to requests from client/servers
        ZMQ.Socket socket = context.socket( ZMQ.REP );
        socket.bind("tcp://localhost:" + Long.toString( port_1 ) );//5555
        
        // Publishing to servers
        final ZMQ.Socket socket_pub = context.socket( ZMQ.PUB );
        socket_pub.bind("tcp://localhost:" + Long.toString( port_3 ) );//6667
        
        //socket_toserver = context.socket( ZMQ.REQ );
        
        // This socket is for pushing to all of the servers
        socket_toserver = context.socket( ZMQ.PUSH );
        socket_toserver.bind("tcp://localhost:" + Long.toString( port_2 ) );//6666
        
        quemap = new HashMap<String,ArrayDeque<String>>();
        donemap = new HashMap<String,Integer>();
        taskmap = new HashMap<Long,mTask>();
        
        Runtime rt = Runtime.getRuntime();
        // Handle termination signal cleanly
        /*rt.addShutdownHook( new Thread() {
            @Override
            public void run() {
                //ZMQ.Context ct2 = ZMQ.context( 1 );
                //ZMQ.Socket socket_pub2 = ct2.socket( ZMQ.PUB );
                //socket_pub2.bind("tcp://localhost:6667");
                
                /*ZMQ.Socket socket_to_man = ct2.socket( ZMQ.REQ ); // send a message to ourselves
                socket_to_man.connect( "tcp://localhost:5555" );
                String line = "{\"type\":\"shutdown\"}";
                socket_to_man.send( line.getBytes( ZMQ.CHARSET ), 0 );
                byte[] reply = socket_to_man.recv(0);
                socket_to_man.close();
                ct2.term();
                System.out.println("Sent command to stop to servers");/
                
                socket_pub.sendMore("ALL");
                socket_pub.send("{\"type\":\"stop\"}");
                socket_pub.close();
                try {
                    Thread.sleep( 500 );
                }
                catch( Exception e ) {}
                //ct2.term();
                
            }
        } );*/
        
        // Thread.sleep( 500 ); // wait half a second for giggles
        // Startup children
        /*Process s1 = rt.exec( "java -jar server.jar" );
        Process s2 = rt.exec( "java -jar server.jar" );
        Process s3 = rt.exec( "java -jar server.jar" );*/
        for( int i=0;i<min_servers;i++ ) {
            //java.lang.ProcessBuilder pb = new java.lang.ProcessBuilder( "java", "-jar", "server.jar", conf_filename );
            rt.exec( "bash start_server" );
            //Process p = pb.start();
            //StreamGobbler a = new StreamGobbler( p.getErrorStream(), "ERROR" ); a.start();
            //StreamGobbler b = new StreamGobbler( p.getInputStream(), "OUTPUT" ); b.start();
        }
        
        sdt.sd_notify( 0, "READY=1" );
        sdt.sd_notify( 0, "STATUS=Ready" );
        
        // listen and respond to requests from client, and for updates from servers
        boolean running = true;
        while( running && !Thread.currentThread().isInterrupted() ) {
            byte[] json_bytes = socket.recv( 0 );
            
            String json_str = new String( json_bytes, ZMQ.CHARSET );
            
            //System.out.println("Received: " + json_str + "\n" );
            
            JSONObject json = (JSONObject) JSONValue.parse( json_str );
            
            String type = (String) json.get( "type" );
            
            if( type.equals( "testpub" ) ) {
                socket.send( "ok".getBytes( ZMQ.CHARSET ), 0 );
                socket_pub.sendMore("ALL");
                socket_pub.send("{\"type\":\"testpub\"}");
            }
            // This command can never be received unless we skip shutdown
            else if( type.equals( "shutdown" ) ) {
                socket.send( "ok".getBytes( ZMQ.CHARSET ), 0 );
                response_delayed = true;
                socket_pub.sendMore("ALL");
                socket_pub.send("{\"type\":\"stop\"}");
                
                /*
                In theory instead of the following stuff we could simply wait for our subprocesses to finish.
                That is not done here, because starting the servers as subprocesses is optional and may not always
                be how they are started.
                
                In simple case, s1.waitFor(), s2.waitFor(), s3.waitFor() etc
                */
                
                Thread.sleep( 500 ); // sleep for half a second, allowing threads to report their status
                // respond to status messages from clients
                int servers_responded = 0;
                
                System.out.println("Receiving goodbye from servers");
                while( true ) {
                    json_bytes = socket.recv( ZMQ.DONTWAIT );
                    if( json_bytes == null ) break;
                    
                    socket.send( "ok".getBytes( ZMQ.CHARSET ), 0 );
                    json_str = new String( json_bytes, ZMQ.CHARSET );
                    System.out.println( "Got " + json_str );
                    json = (JSONObject) JSONValue.parse( json_str );
                    type = (String) json.get( "type" );
                    if( type.equals( "goodbye" ) ) {
                        System.out.println("Received goodbye");
                        servers_responded++;
                    }
                }
                System.out.println("Waiting for all servers to finish");
                if( servers_responded < server_count ) {
                    // wait for servers to finish
                    while( servers_responded < server_count ) {
                        json_bytes = socket.recv( 0 );
                        socket.send( "ok".getBytes( ZMQ.CHARSET ), 0 );
                        
                        json_str = new String( json_bytes, ZMQ.CHARSET );
                        
                        System.out.println( "Got " + json_str );
                        json = (JSONObject) JSONValue.parse( json_str );
                        type = (String) json.get( "type" );
                        if( type.equals( "goodbye" ) ) {
                            servers_responded++;
                        }
                    }
                }
                System.out.println("Servers finished");
                running = false;
                
                // socket_pub.sendMore("server-1")
            }
            else if( type.equals( "start_report" ) ) {
                System.out.println("Received: " + json_str + "\n" );
                //String request = "world";
                String server_result = server_start( json_str );
                
                JSONObject srJson = (JSONObject) JSONValue.parse( server_result );
                // pull out the id of this running process and track it internally
                //long server_id     = (long) srJson.get("server_id");
                long processing_id = (long) srJson.get("processing_id");
                long server_id = 1;
                addQue( server_id, processing_id );
                
                System.out.println("start_report response: " + server_result + "\n");
                
                socket.send( server_result.getBytes( ZMQ.CHARSET ), 0 );
            }
            else if( type.equals( "get_update" ) ) {
                // check internally for status update
                
                //long server_id     = (long) json.get("server_id");
                long server_id = 1;
                long processing_id = (long) json.get("processing_id");
                
                JSONObject obj = new JSONObject();
                
                Deque<String> report_que = getQue( server_id, processing_id );
                if( report_que != null ) {
                    Object[] lines = report_que.toArray();
                    JSONArray arr = new JSONArray();
                    for( int i=0;i<lines.length;i++ ) {
                        arr.add( new String( (String) lines[ i ] ) );
                    }
                    obj.put( "lines", arr );
                    report_que.clear();
                }
                
                if( isDone( server_id, processing_id ) ) {
                    obj.put("done", 1 );
                    removeQue( server_id, processing_id );
                }
                else {
                    obj.put("done", 0 );
                }
                
                String some_updates = obj.toJSONString();
                
                //System.out.println( "Sent" + some_updates + "\n" );
                
                socket.send( some_updates.getBytes( ZMQ.CHARSET ), 0 );
            }
            else if( type.equals( "report_update" ) ) {
                System.out.println("Received: " + json_str + "\n" );
                // update of a running report coming from a server
                String line          = (String) json.get("line");
                long server_id     = (long) json.get("server_id");
                long processing_id = (long) json.get("processing_id");
                ArrayDeque<String> report_que = getQue( server_id, processing_id );
                if( line != null && report_que != null ) report_que.add( line );
                
                String ok = "ok";
                socket.send( ok.getBytes( ZMQ.CHARSET ), 0 );
            }
            else if( type.equals( "task_start" ) ) {
                System.out.println("Received: " + json_str + "\n" );
                // update of a running report coming from a server
                long server_id     = (long) json.get("server_id");
                long processing_id = (long) json.get("processing_id");
                mTask task = taskmap.get( processing_id );
                task.start();
                num_tasks++;
                update_systemd();
                
                String ok = "ok";
                socket.send( ok.getBytes( ZMQ.CHARSET ), 0 );
            }
            else if( type.equals( "status" ) ) {
                // Return basic status information about the manager:
                // - Number and ids of current running servers
                // - Currently running tasks
                // - Queued tasks waiting to be started
                // - How long it took to run the last N tasks
                
                List<mTask> waiting_tasks = new ArrayList<mTask>();
                List<mTask> running_tasks = new ArrayList<mTask>();
                //Map<Long,mTask> finished_tasks = new HashMap<Long,mTask>();
                List<mTask> finished_tasks = new ArrayList<mTask>();
                
                for( Long procid : taskmap.keySet() ) {
                    mTask task = taskmap.get( procid );
                    int status  = task.status();
                    if( status == 0 ) waiting_tasks.add( task );
                    if( status == 1 ) running_tasks.add( task );
                    if( status == 2 ) finished_tasks.add( task );
                    /*if( status == 2 ) {
                        long finish_time = task.get_finish_time();
                        finished_tasks.put( finish_time, task );
                    }*/
                }
                
                JSONObject info = new JSONObject();
                
                JSONArray waiting = new JSONArray();
                info.put( "waiting", waiting );
                for( mTask task : waiting_tasks ) {
                    waiting.add( task.toString() );
                }
                
                JSONArray runningA = new JSONArray();
                info.put( "running", runningA );
                for( mTask task : running_tasks ) {
                    runningA.add( task.toString() );
                }
                
                JSONArray finished = new JSONArray();
                info.put( "finished", finished );
                Comparator<mTask> byFinish = new Comparator<mTask>() {
                    public int compare( mTask t1, mTask t2 ) {
                        long finish1 = t1.get_finish_time();
                        long finish2 = t2.get_finish_time();
                        if( finish1 > finish2 ) return 1;
                        if( finish2 < finish1 ) return -1;
                        return 0;
                    }
                };
                Collections.sort( finished_tasks, byFinish );
                //SortedSet<Long> keys = new TreeSet<Long>( finished_tasks.keySet() );
                for( mTask task : finished_tasks ) {
                    //long finish = task.get_finish_time();
                    finished.add( task.toString() );
                }
                
                String jsonStr = info.toJSONString();
                socket.send( jsonStr.getBytes( ZMQ.CHARSET ), 0 );
            }
            else if( type.equals( "report_done" ) ) {
                System.out.println("Received: " + json_str + "\n" );
                long server_id     = (long) json.get("server_id");
                long processing_id = (long) json.get("processing_id");
                
                mTask task = taskmap.get( processing_id );
                task.finish();
                num_tasks--;
                update_systemd();
                
                //removeQue( server_id, processing_id );
                setDone( server_id, processing_id );
                // message from a server stating that a running report is now finished
                String ok = "ok";
                socket.send( ok.getBytes( ZMQ.CHARSET ), 0 );
            }
            else if( type.equals( "assign_server_id" ) ) {
                System.out.println("Received: " + json_str + "\n" );
                // message from a server stating that a running report is now finished
                String ok = "{\"server_id\": " + Long.toString( next_server_id++ ) + "}";
                
                System.out.println("Sending " + ok + "\n" );
                server_count++;
                
                socket.send( ok.getBytes( ZMQ.CHARSET ), 0 );
            }
            else if( type.equals("process_started" ) ) {
                System.out.println("Process started");
                String ok = "ok";
                socket.send( ok.getBytes( ZMQ.CHARSET ), 0 );
            }
            else {
                System.out.println("Received unknown type: " + type );
                String ok = "ok";
                socket.send( ok.getBytes( ZMQ.CHARSET ), 0 );
            }
        }
        if( response_delayed ) {
            while( true ) {
                byte[] json_bytes = socket.recv();
                String json_str = new String( json_bytes, ZMQ.CHARSET );
                System.out.println( "Got " + json_str );
                JSONObject json = (JSONObject) JSONValue.parse( json_str );
                String type = (String) json.get( "type" );
                if( type.equals( "final" ) ) {
                    socket.send( "done".getBytes( ZMQ.CHARSET ), 0 );
                    System.out.println("Received final");
                    break;
                }
                else {
                    socket.send( "ok".getBytes( ZMQ.CHARSET ), 0 );
                }
            }
        }
        
        socket_pub.close();
        socket_toserver.close();
        socket.close();
        context.term();
        
        sdt.sd_notify( 0, "STATUS=Stopped" );
        
        System.exit( 0 );
    }
    
    public static void update_systemd() {
        if( num_tasks == 0 ) {
            sdt.sd_notify( 0, "STATUS=Ready for tasks - " + Long.toString( min_servers ) + " servers" );
        }
        else {
            sdt.sd_notify( 0, "STATUS=Running tasks - " + Integer.toString( num_tasks ) + "/" + Long.toString( min_servers ) );
        }
    }
    
    public static boolean isDone( long server_id, long processing_id ) {
        //String lookup = "" + Long.toString( server_id ) + "_" + Long.toString( processing_id );
        
        //System.out.println("Checking if proc id is done: " + Long.toString( processing_id ) + "\n" );
        String lookup = Long.toString( processing_id );
        
        Integer res = donemap.get( lookup );
        if( res != null ) {
        	donemap.remove( lookup );
        	return true;
        }
        else {
        	return false;
        }
    }
    
    public static void setDone( long server_id, long processing_id ) {
        //String lookup = "" + Long.toString( server_id ) + "_" + Long.toString( processing_id );
        String lookup = Long.toString( processing_id );
        donemap.put( lookup, 1 );
    }
    
    public static ArrayDeque<String> getQue( long server_id, long processing_id ) {
        //String lookup = "" + Long.toString( server_id ) + "_" + Long.toString( processing_id );
        String lookup = Long.toString( processing_id );
        return quemap.get(lookup);
    }
    
    public static void removeQue( long server_id, long processing_id ) {
        //String lookup = "" + Long.toString( server_id ) + "_" + Long.toString( processing_id );
        String lookup = Long.toString( processing_id );
        quemap.remove(lookup);
    }
    
    public static void addQue( long server_id, long processing_id ) {
        //String lookup = "" + Long.toString( server_id ) + "_" + Long.toString( processing_id );
        String lookup = Long.toString( processing_id );
        quemap.put( lookup, new ArrayDeque<String>() );
    }
    
    public static String server_start( String json_str ) {
        System.out.println("Sending to server: " + json_str + "\n");
        
        JSONObject json = (JSONObject) JSONValue.parse( json_str );
        long procid = next_processing_id++;
        
        mTask newtask = new mTask( procid, (String) json.get("command") );
        taskmap.put( procid, newtask );
        
        json.put( "processing_id", procid );
        String json2 = json.toJSONString();
        
        socket_toserver.send( json2.getBytes( ZMQ.CHARSET ), 0 );
        //byte[] reply = socket_toserver.recv(0);
        //String reply_str = new String( reply, ZMQ.CHARSET );
        String reply_str = "{\"processing_id\":" + Long.toString( procid ) + ",\"server_id\":1}";
        
        System.out.println("Got from server " + reply_str + "\n" );
        return reply_str;
    }
    
    static {
        //System.loadLibrary("SystemDTools");
        System.load("/opt/bfms_trunk/birt_reporting/jni_systemd/libSystemDTools.so");
    }
}