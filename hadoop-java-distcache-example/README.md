hadoop-cache-distcache-example
==============================

The "Hello World" example in hadoop incorporating the joys of the distributed cache.

Without any options, this is a standard hadoop word count example that counts the number of unique words from an input file. In that case, you would run it like this:

hadoop jar ./hadoop-java-distcache-example-1.0.jar  st.ata.vcc.examples.hadoop.mapred.wordcount.WordCountAndReplace -replace /user/hdfs/examples/distcache/replaceList.txt /user/hdfs/examples/input /user/hdfs/examples/output


You can add the "-replace <hdfs file name>" option. The file is expected to contain a series of word pairs, one pair per line i.e.
 
 <word1> <word1 replacement>
 <word2> <word2 replacement>
 :
 :

The example will then replace the words in the input file with the words in the list. You would then run it like this:

hadoop jar ./hadoop-java-distcache-example-1.0.jar  st.ata.vcc.examples.hadoop.mapred.wordcount.WordCountAndReplace -replace /user/hdfs/examples/distcache/replaceList.txt /user/hdfs/examples/input /user/hdfs/examples/output-replace
