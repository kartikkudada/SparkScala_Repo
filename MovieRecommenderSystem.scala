package com.upxacademy.projects

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger

case class Movies(movieid: Int, moviename: String, category: String)
case class Ratings(userid: Int, movieid: Int, rating: Int)
object MovieRecommenderSystem {
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new SparkConf().setAppName("Movie Recommender System")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    val movies = sc.textFile("movies.dat").map(_.split("::"))
    val moviesRDD = movies.map(x => Movies( x(0).trim.toInt, x(1), x(2)) )
    val moviesDF = moviesRDD.toDF().filter($"category" like "Animation%")
    
   // moviesDF.show()
    val ratingsDF = sc.textFile("ratings.dat").map(_.split("::")).map(
        x => Ratings(x(0).toInt , x(1).toInt,x(2).toInt)
      ).toDF().where("rating >= 4")
   
    var joinedMovieDF = moviesDF.join(ratingsDF,moviesDF("movieid") === ratingsDF("movieid"))
    joinedMovieDF.show()
    
  }
 }