//19AIE214 Big Data Analytics
//Assignments

//Date: May 2 2021

//Kabilan N
//CB.EN.U4AIE19033

// 2. Use file ‘wikisample.txt’ and find the following details

// a) The unique and total word count of the file.

val wikisampleRdd = sc.textFile("Data\\wikisample.txt")
//wikisampleRdd: org.apache.spark.rdd.RDD[String] = Data\wikisample.txt MapPartitionsRDD[1] at textFile at <console>:24

val WikionlyLettersRdd=wikisampleRdd.map(_.replaceAll("[^A-Za-z\\s]*",""))
//WikionlyLettersRdd: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[2] at map at <console>:26

val WikiReducedRdd = WikionlyLettersRdd.map(l=> l.toLowerCase)
//WikiReducedRdd: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[3] at map at <console>:28

val WikiUniqueRdd = WikiReducedRdd.flatMap(_.split("\\s+")).map(x=>(x,1)).reduceByKey(_ + _)
//WikiUniqueRdd: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[6] at reduceByKey at <console>:30

WikiUniqueRdd.toDF.show()
// [Stage 0:=============================>                             (1 + 1) / 2]21/05/02 19:49:32 WARN Executor: Managed memory leak detected; size = 5246008 bytes, TID = 2
// +---------------+---+
// |             _1| _2|
// +---------------+---+
// |       vecindad|  4|
// |            mjs|  1|
// |           bone|154|
// |     glorifying|  2|
// |          mller| 23|
// |       bodongpa|  1|
// |       biaystok|  1|
// |          folan|  1|
// |           gpsp|  1|
// |          sympy|  1|
// |   outplantings|  1|
// |        pelotas|  2|
// |         nothin|  2|
// |   sundayschool|  1|
// |         cuffay|  2|
// |         odwyer|  2|
// |     strippable|  1|
// |australianbuilt|  1|
// |     henseforth|  1|
// |            hem|  6|
// +---------------+---+
// only showing top 20 rows


val WikiUnigramcnt = WikiUniqueRdd.map{case(x,y)=> y}.sum()
// WikiUnigramcnt: Double = 6234346.0


// b) The count of ‘high’ and the joint count of ‘high profile’ and ‘high school’. Also find relative frequencies (n-gram probabilities) of these pairs.

val WikiHighRddNum = (WikiUniqueRdd.lookup("high")(0)).toDouble
// WikiHighRddNum: Double = 39473.0

val WikiHighRdd = (WikiUniqueRdd.lookup("high")(0)).toDouble/WikiUnigramcnt
// WikiHighRdd: Double = 0.006331538223897102

val WikiBGReducedRdd= WikiReducedRdd.map( r=> r.split("\\s+"))
// WikiBGReducedRdd: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[24] at map at <console>:30

val bigrams=WikiBGReducedRdd.map(l=>l.sliding(2)).flatMap{identity}.map{_.mkString(" ")}.map(s=>(s,1)).reduceByKey(_+_)
// bigrams: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[29] at reduceByKey at <console>:32

bigrams.toDF.show()
// [Stage 12:=============================>                            (1 + 1) / 2]21/05/02 20:08:20 WARN Executor: Managed memory leak detected; size = 83631140 bytes, TID = 14
// +--------------------+---+
// |                  _1| _2|
// +--------------------+---+
// |          to karelia|  1|
// |       appear within|  1|
// |         by peaceful|  1|
// | twoday invitational|  1|
// |   ensuring existing|  1|
// |          his london|  1|
// |     moderate height|  1|
// |   perform political|  1|
// |          heard live|  1|
// |    who demonstrated|  1|
// |   hybrid orangutans|  1|
// |proportional redu...|  1|
// |          and davids|  1|
// |        dolittle and|  1|
// |         ignites the|  2|
// |       town resulted|  1|
// |      town fireworks|  1|
// |            wits and|  1|
// |       apple located|  1|
// |nederland performing|  1|
// +--------------------+---+
// only showing top 20 rows


val WikiBigramcnt = bigrams.map{case(x,y)=> y}.sum()
// WikiBigramcnt: Double = 6169072.0

val WikiHighprofilecnt = (bigrams.lookup("high profile")(0))
// WikiHighprofilecnt: Int = 158

val WikiHighprofileRdd = (bigrams.lookup("high profile")(0))/WikiBigramcnt
// WikiHighprofileRdd: Double = 2.5611631700845766E-5

val WikiHighschoolcnt = (bigrams.lookup("high school")(0))
// WikiHighschoolcnt: Int = 12604

val WikiHighschoolRdd=(bigrams.lookup("high school")(0)).toDouble/WikiBigramcnt
// WikiHighschoolRdd: Double = 0.002043094974414304


// c) Number of lines with word ‘mountain’ and ‘God’, Also find the count of the word mountain.

val WikiMountGodRdd = WikiReducedRdd.filter{case(a)=>(a.contains("mountain")& a.contains("god"))}.count()
// WikiMountGodRdd: Long = 15

val WikiMountcnt = (WikiUniqueRdd.lookup("mountain")(0)).toDouble
// WikiMountcnt: Double = 2072.0


// d) Find the words that has the top 10 occurrences and the least 10 occurrences.

val WikiTop10Rdd = sc.parallelize(WikiUniqueRdd.sortBy(_._2,false).take(10))
// 21/05/02 20:22:27 WARN Executor: Managed memory leak detected; size = 5448528 bytes, TID = 26
// WikiTop10Rdd: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[41] at parallelize at <console>:32

WikiTop10Rdd.toDF.show()
/*+---+------+
| _1|    _2|
+---+------+
|the|477839|
| of|228418|
|and|195358|
| in|173709|
| to|140329|
|  a|138315|
| is| 72357|
|was| 60988|
| as| 56429|
|for| 52409|
+---+------+
*/

val WikiLeast10Rdd = sc.parallelize(WikiUniqueRdd.sortBy(_._2).take(10))
// 21/05/02 20:26:01 WARN Executor: Managed memory leak detected; size = 15768522 bytes, TID = 39
// WikiLeast10Rdd: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[50] at parallelize at <console>:32

WikiLeast10Rdd.toDF.show()
/*+---------------+---+
|             _1| _2|
+---------------+---+
|            mjs|  1|
|       bodongpa|  1|
|       biaystok|  1|
|          folan|  1|
|           gpsp|  1|
|          sympy|  1|
|   outplantings|  1|
|   sundayschool|  1|
|     strippable|  1|
|australianbuilt|  1|
+---------------+---+*/

// e) Find how many lines exit which do not have numbers in them.
val WithwithoutNumRdd = wikisampleRdd.filter(_.exists(_.isDigit)!=true).count
// WithwithoutNumRdd: Long = 20710



