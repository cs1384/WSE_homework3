===== operations =====

Compile:
$ javac -cp lib/jsoup-1.8.1.jar:lib/commons-io-2.4.jar src/edu/nyu/cs/cs2580/*.java

Mining:
$ java -cp src edu.nyu.cs.cs2580.SearchEngine --mode=mining --options=conf/engine.conf

Indexing:
$ java -cp src:lib/jsoup-1.8.1.jar:lib/commons-io-2.4.jar edu.nyu.cs.cs2580.SearchEngine --mode=index --options=conf/engine.conf

Serving:
$ java -cp src:lib/commons-io-2.4.jar -Xmx512m edu.nyu.cs.cs2580.SearchEngine --mode=serve --port=25816 --options=conf/engine.conf

===== testing  ======

$ curl "http://localhost:25816/search?query=web+%22focused+crawler%22&ranker=COMPREHENSIVE&format=text"

$ curl "http://localhost:25816/search?query=google+%22web+searching%22&ranker=COMPREHENSIVE&format=text"

$ curl "http://localhost:25816/search?query=any+%22north+american%22&ranker=COMPREHENSIVE&format=text"


===== build up the wroking repository =====

$ git clone https://github.com/cs1384/WSE_homework3.git
$ cd <where you store this directory>
$ mkdir conf
$ cp engine_ori.conf conf/engine.conf
$ cd data
$ mkdir index
$ ln -s <path to your local wiki folder>

