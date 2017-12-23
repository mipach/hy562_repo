val rdd = sc.textFile("/home/mipach/accidents.dat")

//reduce the file so my computer won't crash
val reduced_rdd = rdd.map(x => x.split(" ")).map(x => x.take(7).mkString(" "))

val accidents = reduced_rdd.collect

//initialize the Apriori and run it

//stage 1 map and reduce
val alg = new AprioriAlgorithm(accidents)

//spark splits the chunks based on the workers automatically and does the sum
alg.runApriori(0.35,0.60)

val candidate = alg.toRetItems

//stage 2 map

val toCheck = accidents.map(x => x.split(" ").toSet)

var x2 = 0.0
var new_set = collection.mutable.Map(candidate.toSeq: _*)
new_set = new_set.map(x => (x._1,x2))

//increment the new_set(Set(value)) by one each time we find the items in the initial dataset (toCheck)

//phase2 map
for(st <- new_set) {
 toCheck.map{ x => if(st._1.diff(x).isEmpty == true) new_set(st._1) += 1}

}

//filter phase 2 
// set the support by counting the elements in toCheck and then 
val total_count = toCheck.size

val items_with_support = new_set.map(x => (x._1, (x._2/total_count)*100))

val support = 50.0

val toRet = items_with_support.filter(x => x._2 >= support)
