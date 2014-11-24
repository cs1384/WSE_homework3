PageRank:
	Find the optimal lambda, iteration value.
	We do grid search in lambda = [0.1, 0.9], iteration = [1, 2]
	The result is:
			lambda			iteration				Spearman
		1)		0.1				1				0.45537
		2)		0.1				2				0.45347
		3)		0.9				1				0.45822
		4)		0.9				2				0.45033
	Hence we set lambda = 0.9, iteration = 1.

Compare PageRank and NumViews:
	In our optimal setting, the Spearman coefficient is 0.45822.

===== operations =====

Compile:
$ javac -cp lib/jsoup-1.8.1.jar:lib/commons-io-2.4.jar src/edu/nyu/cs/cs2580/*.java

Mining:
$ java -cp src edu.nyu.cs.cs2580.SearchEngine --mode=mining --options=conf/engine.conf

Spearman:
$ java -cp src edu.nyu.cs.cs2580.Spearman ./data/index/pageRank.idx ./data/index/numViews.idx

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

