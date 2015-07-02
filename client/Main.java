package client;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.zeromq.ZMQ;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Main {
    static ZMQ.Context context;
    static ZMQ.Socket socket;
    static boolean debug = false;
    
    static long port_1 = 5555;
    static long port_2 = 6666;
    static long port_3 = 6667;
    static long min_servers = 3;
    static long max_servers = 4;
    // After this many seconds of inactivity, servers beyond min_servers will be shut down
    static long server_inactive_timeout_seconds = 1000;
    static String looping_cmd = "perl test.pl";
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
        if( args.length == 0 ) {
            System.out.println("You need to specify a configuration file" );
            return;
        }
        String conf_filename = args[0];
        if( !read_conf( conf_filename ) ) { return; }
        
        context = ZMQ.context(1);
        socket = context.socket( ZMQ.REQ );
        socket.connect( "tcp://localhost:" + Long.toString( port_1 ) ); // 5555
        
        socket.setBacklog( 5 );
        
        if( args.length > 1 ) {
            String arg1 = args[ 1 ];
            // operations:
            // -show_servers // returns a list of server_ids
            // -run_report // returns a server_id + run_id
            // -get_update( server_id, run_id )
            if( arg1.equals( "-json" ) ) {
                String json_str = null;
                if( args.length > 2 ) {
                    json_str = args[ 2 ];
                }
                else {
                    json_str = getJsonFromStdin();
                }
                Object json = null;
                //try {
                    json = JSONValue.parse( json_str );
                    doRequest( json_str );
                //}
                //catch( ParseException pe ) {
                //    System.out.println("invalid json: " + pe );
                //}
            }
            if( arg1.equals( "start" ) ) {
                String json_str = "{\"type\":\"start_report\"}";
                doRequest( json_str );
            }
            if( arg1.equals( "gb" ) ) {
                String json_str = "{\"type\":\"goodbye\"}";
                
                socket.send( json_str.getBytes( ZMQ.CHARSET ), 0 );
            
                byte[] json_bytes2 = socket.recv( 0 );
            }
            if( arg1.equals( "status" ) ) {
                String json_str = "{\"type\":\"status\"}";
                
                socket.send( json_str.getBytes( ZMQ.CHARSET ), 0 );
            
                String res = socket.recvStr( 0 );
                JSONObject stat = (JSONObject) JSONValue.parse( res );
                JSONArray waiting = (JSONArray) stat.get("waiting");
                JSONArray finished = (JSONArray) stat.get("finished");
                JSONArray running = (JSONArray) stat.get("running");
                
                System.out.println("Waiting:");
                for( Object w : waiting ) {
                    String w2 = (String) w;
                    System.out.println( w2 );
                }
                
                System.out.println("\nFinished:");
                for( Object f : finished ) {
                    String f2 = (String) f;
                    System.out.println( f2 );
                }
                
                System.out.println("\nRunning:");
                for( Object r : running ) {
                    String r2 = (String) r;
                    System.out.println( r2 );
                }
                
                //System.out.println( res );
            }
            if( arg1.equals("shutdown" ) ) {
                String json_str = "{\"type\":\"shutdown\"}";
                
                socket.send( json_str.getBytes( ZMQ.CHARSET ), 0 );
            
                byte[] json_bytes2 = socket.recv( 0 );
                
                while( true ) {
                    socket.send( "{\"type\":\"final\"}".getBytes( ZMQ.CHARSET ), 0 );
                    String res = socket.recvStr();
                    if( res.equals( "done" ) ) break;
                    Thread.sleep(1000);
                }
                
                //doRequest( json_str );
            }
            if( arg1.equals("testpub" ) ) {
                String json_str = "{\"type\":\"testpub\"}";
                
                socket.send( json_str.getBytes( ZMQ.CHARSET ), 0 );
            
                byte[] json_bytes2 = socket.recv( 0 );
            
                //doRequest( json_str );
            }
        }
        
        if( socket != null ) socket.close();
        context.term();
    }
    
    public static String getJsonFromStdin() throws IOException {
        StringBuilder sb = new StringBuilder();
        String input;
        BufferedReader br = new BufferedReader( new InputStreamReader( System.in ) );
        while( ( input = br.readLine() ) != null ) {
            //System.out.println("Got [" + input + "]" );
            //if( input.equals("\n") ) break;
            sb.append( input );
        }
        String json_str = sb.toString();
        return json_str;
    }
    
    public static void doRequest( String json ) throws InterruptedException {
        if(debug) System.out.println("Starting new report via manager");
        
        
        socket.send( json.getBytes( ZMQ.CHARSET ), 0 );
        
        byte[] json_bytes = socket.recv( 0 );
        String json_str = new String( json_bytes, ZMQ.CHARSET );
        JSONObject jsonres = (JSONObject) JSONValue.parse( json_str );
            
        if(debug) System.out.println("Result of starting report " + json_str + "\n" );
        
        long server_id = (long) jsonres.get( "server_id" );
        long processing_id = (long) jsonres.get( "processing_id" );
        
        int stop = 0;
        while( stop==0 ) {
            String updatecmd = "{\"type\":\"get_update\",\"server_id\":" + Long.toString( server_id ) +
                ",\"processing_id\":" + Long.toString( processing_id ) + "}";
            //System.out.println("Requesting an update\n");
            //if(debug) System.out.print(".");
            socket.send( updatecmd.getBytes( ZMQ.CHARSET ), 0 );
            
            byte[] json_bytes2 = socket.recv( 0 );
            String json_str2 = new String( json_bytes2, ZMQ.CHARSET );
            //System.out.println("Update received:" + json_str2 + "\n");
            
            JSONObject json2 = (JSONObject) JSONValue.parse( json_str2 );
            
            long done = (long) json2.get("done");
            if( done > 0 ) {
                if(debug) System.out.println("Received done\n");
                stop = 1;
            }
            JSONArray lines = (JSONArray) json2.get("lines");
            
            if( lines != null ) {
                if( lines.size() > 0 ) {
                    if(debug) System.out.println("Update received:" + json_str2 + "\n");
                }
                
                for( int i=0;i<lines.size();i++ ) {
                    String line = (String) lines.get(i);
                    if(debug) System.out.println( "Line: " + line + "\n" );
                    else System.out.println( line );
                }
            }
            
            Thread.sleep( 500 );
        }
        
        socket.close();
        socket = null;
        
        // periodically poll for updates to the running report
    }
}