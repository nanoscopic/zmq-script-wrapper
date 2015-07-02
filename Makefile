all: client server manager

client: client.jar

server: server.jar

manager: manager.jar

client.jar: client_Makefile client/Main.java
	make -f client_Makefile

server.jar: server_Makefile server/Main.java
	make -f server_Makefile

manager.jar: manager_Makefile manager/Main.java manager/mTask.java
	make -f manager_Makefile

client_Makefile: client_specs.xml
	perl generate_makefiles.pl

server_Makefile: server_specs.xml
	perl generate_makefiles.pl

manager_Makefile: manager_specs.xml
	perl generate_makefiles.pl

clean:
	make -f manager_Makefile clean
	make -f client_Makefile clean
	make -f server_Makefile clean
	$(RM) client_Manifest.txt server_Manifest.txt manager_Manifest.txt server_Makefile client_Makefile manager_Makefile

cleanall: clean
	$(RM) client.jar server.jar manager.jar
