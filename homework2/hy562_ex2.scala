val pagecounts = sc.textFile("/home/mipach/Downloads/pagecounts-20160101-000000")

case class Log(code:String,title:String,hits:Long,size:Long)

//q1
pagecounts.take(15).foreach(println)

//q2
pagecounts.count

//q3
val LogRDD = pagecounts.map(x => x.split(" ")).map(x => Log(x(0),x(1),x(2).toLong,x(3).toLong))
val min_page = LogRDD.map(x => x.size).min
val max_page = LogRDD.map(x => x.size).max
val avg_page = LogRDD.map(x => x.size).sum / LogRDD.count

//q4
val largest_records = LogRDD.filter(x => x.size == max_page )
largest_records.foreach(println)

//q5
//need fixing
val test2 = largest_records.map( x => (x.code,1))
val x = sc.parallelize(test2)
x.reduceByKey((accum,n) => (accum+n)).sortBy(-_._2).head()

//q6
val max_page_title = LogRDD.map(x => x.title.length).max
val max_title_list = LogRDD.filter(x => x.title.length == max_page_title)

//q7
val size_gr_from_avg = LogRDD.filter(x => x.size > avg_page )

//q8
val views_per_project = LogRDD.map(x => (x.code, x.hits)).reduceByKey(_+_)

//q9
val most_popular = views_per_project.sortBy(-_._2)
most_popular.take(10).foreach(println)

//q10
val t10 = LogRDD.filter(x =>( x.title.contains("The_"))).map(x => (x.code,x.title.split("_"))).filter(x => x._2(0).equals("The")).collect
val t10_2 = t10.filterNot(x => x._1.contains("en")).size
//val t10_2 = t10.filter(x => x._1.contains("en") == false).count //this only if we don't collect t10

/q11
val single_view_count = LogRDD.filter(x => x.hits == 1).count
val total = LogRDD.count
val percentage = single_view_count.toDouble / total.toDouble * 100.0

//q12
//unique words in titles
val q12 = LogRDD.map(x => x.title)
val q12_2 =q12.filter(x => x.matches("[A-Za-z_:]+"))
val q12_3 = q12_2.flatMap(x => x.split("_")).map(x => x.toLowerCase) //all the terms 
val res = q12_3.distinct
res.count

//q13
val q13 = q12_3.map(x => (x,1)).reduceByKey( _+_ )
val res = q13.sortBy(-_._2)
res.take(10).foreach(println)
