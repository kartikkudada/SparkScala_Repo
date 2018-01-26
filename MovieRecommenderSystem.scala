package com.upxacademy.projects

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class Movies(movieid: Int, moviename: String, category: String)
case class Ratings(userid: Int, movieid: Int, rating: Int)
case class User(userid:Int , sex : String, age:Int )
object MovieRecommenderSystem {
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new SparkConf().setAppName("Movie Recommender System")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import org.apache.spark.sql.SQLContext.implicits._
    
    val movies = sc.textFile("movies.dat").map(_.split("::"))
    val moviesRDD = movies.map(x => Movies( x(0).trim.toInt, x(1), x(2)) )
    val moviesDF = moviesRDD.toDF().filter($"category" like "Animation%")
    
   // moviesDF.show()
    val ratingsDF = sc.textFile("ratings.dat").map(_.split("::")).map(
        x => Ratings(x(0).toInt , x(1).toInt,x(2).toInt)
      ).toDF().where("rating >= 4")
   
      //Animated movie whose ratings are 4 and above
    var joinedMovieDF = moviesDF.join(ratingsDF,moviesDF("movieid") === ratingsDF("movieid"))
    joinedMovieDF.show()
    
    // Reading User.dat file 
    val user = sc.textFile("users.dat").map(_.split("::"))
    val userRDD = user.map(x => User(x(0).toInt,x(1),x(2).toInt))
    val userDF = userRDD.toDF()
    
    userDF.show()
    val groupByAgeDF = userDF.join(ratingsDF,ratingsDF("userid") === userDF("userid"))
    
    // grouping by age to show mean rating given by particular age 
    groupByAgeDF.groupBy("age").mean("rating").show()
    
    // Finding top rating movie name 
    val topRatingsDF =  ratingsDF.filter("rating == 5")
    val topRatingsMovieNameDF = moviesDF.join(topRatingsDF, topRatingsDF("movieid") === moviesDF("movieid"))
    .select("moviename","rating")
    
    
    topRatingsMovieNameDF.show()
  }
 }