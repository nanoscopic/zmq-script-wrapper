ZeroMQ Looping Script Wrapper ( zmq-script-wrapper )

This project is designed to enable simultaneous copies of a simple shell script to run at once continuously, removing the overhead time of loading a script and it's various components.

Example scenario of need:

1. Suppose you have a shell script that takes some command parameters
2. The shell script loads 100mb of libraries each time you run it, and takes some time to do so ( a minute? )
3. The shell script does some work that takes time; outputting progress as it goes
4. The shell script is being called from a web enabled service, such that many users desire to run the script
5. The load time for the script makes the web service suffer
6. It is undesirable for whatever reasons to integrate the shell script into the web service directly

Example solution using zmq-script-wrapper:

1. Tweak the shell script to run in a loop, accepting a single string of input parameters then doing needed work, repeating until 'done' is received as the input parameter.
2. Run the 'manager.jar'.
3. Run as many copies of 'server.jar' as desired ( each will load the shell script once and keep it loaded )
4. Instead of running the shell script directly, call 'client.jar', passing it the parameters as initially done

Real world scenario:

You have a web application written in Python. It uses Java BIRT for reporting. Running BIRT reports is quite slow, and many Java libraries must be loaded each time a BIRT report is loaded. It isn't too difficult to make a shell script that runs a single report, loading BIRT in the process. It is much more difficult to create a Java service that loads multiple simultaneous copies of BIRT ( how to do this is also not documented ). Using this wrapper in combination with a basic script to run BIRT reports allows an easy way to run BIRT reports simultaneously while still receiving status updates on the progress of each running report.