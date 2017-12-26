import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.io.Source
import scala.collection.immutable.List
import scala.collection.immutable.Set
import java.io.File
import scala.collection.mutable.Map



class AprioriAlgorithm(inputFile: Array[String]) {
  var transactions : List[Set[String]] = List()
  var itemSet : Set[String] = Set()
  for (line<-inputFile) {
    val elementSet = line.trim.split(' ').toSet
    if (elementSet.size > 0) {
      transactions = transactions :+ elementSet
      itemSet = itemSet ++ elementSet
    }
  }
  var toRetItems : Map[Set[String], Double] = Map()
  var associationRules : List[(Set[String], Set[String], Double)] = List()

  def getSupport(itemComb : Set[String]) : Double = {
    def withinTransaction(transaction : Set[String]) : Boolean = itemComb
                                                                  .map( x => transaction.contains(x))
                                                                  .reduceRight((x1, x2) => x1 && x2)
    val count = transactions.filter(withinTransaction).size
    count.toDouble / transactions.size.toDouble
  }

  def runApriori(minSupport : Double = 0.15, minConfidence : Double = 0.6) = {
    var itemCombs = itemSet.map( word => (Set(word), getSupport(Set(word))))
                           .filter( wordSupportPair => (wordSupportPair._2 > minSupport))
    var currentLSet : Set[Set[String]] = itemCombs.map( wordSupportPair => wordSupportPair._1).toSet
    var k : Int = 2
    while (currentLSet.size > 0) {
      val currentCSet : Set[Set[String]] = currentLSet.map( wordSet => currentLSet.map(wordSet1 => wordSet | wordSet1))
                                                      .reduceRight( (set1, set2) => set1 | set2)
                                                      .filter( wordSet => (wordSet.size==k))
      val currentItemCombs = currentCSet.map( wordSet => (wordSet, getSupport(wordSet)))
                                        .filter( wordSupportPair => (wordSupportPair._2 > minSupport))
      currentLSet = currentItemCombs.map( wordSupportPair => wordSupportPair._1).toSet
      itemCombs = itemCombs | currentItemCombs
      k += 1
    }
    for (itemComb<-itemCombs) {
      toRetItems += (itemComb._1 -> itemComb._2)
    }
    calculateAssociationRule(minConfidence)
  }

  def calculateAssociationRule(minConfidence : Double = 0.6) = {
    toRetItems.keys.foreach(item =>
      item.subsets.filter( wordSet => (wordSet.size<item.size & wordSet.size>0))
          .foreach( subset => {associationRules = associationRules :+ (subset, item diff subset,
                                                                       toRetItems(item).toDouble/toRetItems(subset).toDouble)
                              }
                  )
    )
    associationRules = associationRules.filter( rule => rule._3>minConfidence)
  }
}

object Homework {
	def main(args: Array[String]) {
		val sc = new SparkContext(new SparkConf().setAppName("Homework 3"))

		val start = System.currentTimeMillis
		val rdd = sc.textFile("/home/mipach/accidents.dat")

		//reduce the file so my computer won't crash
		val reduced_rdd = rdd.map(x => x.split(" ")).map(x => x.take(7).mkString(" "))
	
		val mapped = reduced_rdd.mapPartitions{ x => {
			val t = new AprioriAlgorithm(x.toArray)
			t.runApriori(0.35,0.60)
			t.toRetItems.iterator }
		}
		val collected = mapped.collect
		val candidate = collected.map(x => x._1).distinct
		//stage 2 map

		val toCheck = reduced_rdd.map(x => x.split(" ").toSet)
		val cand = sc.parallelize(candidate)
		val map_phase2 = cand.mapPartitions { x => {
		      var x2 = 0.0
      		      val tmp = cand.map(y => (y,0.0)).collect
      	 	      var new_set:scala.collection.mutable.Map[Set[String],Double] = collection.mutable.Map(tmp.toSeq: _*)
                      new_set = new_set.map(y => (y._1,x2))
                      for(st <- new_set) {     
                      	mapCheck.map{x1 => if(st._1.diff(x1).isEmpty == true) new_set(st._1) += 1}
      		      }
    		    }
    		    new_set.iterator
		}
		val reduced_values = map_phase2.collect
		

		// set the support by counting the elements in toCheck and then 
		val total_count = toCheck.size

		//val items_with_support = new_set.map(x => (x._1, (x._2/total_count)*100))
		val items_with_support = reduced_values.map(x => (x._1,(x._2/total_count)*100))
		val support = 35.0

		val toRet = items_with_support.filter(x => x._2 >= support)
		println(toRet)
		val end = System.currentTimeMillis
		val passed = end - start
		println("Time to compute:" + passed )
	}
}
