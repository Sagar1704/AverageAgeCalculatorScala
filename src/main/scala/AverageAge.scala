package main.scala

import java.util.Calendar

import scala.collection.mutable.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object AverageAge {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("average")
        val sc = new SparkContext(conf)

        val userData = sc.textFile(args(1))
        val userAge = userData.map(user => user.split(",")).map(userInfo => (userInfo(0), getAge(userInfo(9))))
        val usersInfo = userData.map(user => user.split(",")).map(userInfo => (userInfo(0), userInfo(3) + "," + userInfo(4) + "," + userInfo(5)))

        val input = sc.textFile(args(0))
        val inputUsers = input.map(line => line.split("\t")).filter(line => (line.length == 2)).map(profile => getUserMap(profile)).flatMap(profile => (profile)).map(profile => (profile._1, profile._2))

        val friendAge = inputUsers.join(userAge)
        val averageAge = friendAge.map(userMap => userMap.swap).map(userMap => (userMap._1._1, (userMap._2, userMap._1._2))).reduceByKey((pair1, pair2) => (pair1._1 + "," + pair2._1, pair1._2 + pair2._2)).map(userMap => (userMap._1, userMap._2._2 / (userMap._2._1.count(_ == ',') + 1))).sortBy(_._2, false).take(20)

        val output = usersInfo.join(sc.parallelize(averageAge)).sortBy(_._2._2, false).map(userInfo => (userInfo._1 + ", " + userInfo._2._1 + ", " + userInfo._2._2))
        output.saveAsTextFile(".\\output")

    }

    def getUserMap(profile: Array[String]): HashMap[String, String] = {
        val user = profile(0)
        val friends = profile(1).split(",")
        var userMap = new HashMap[String, String]
        for (friend <- friends) {
            userMap.put(friend, user)
        }
        return userMap
    }

    def getDirectFriendAges(friends: Array[String]): Int = {
        return 0
    }

    def getAge(dob: String): Int = {
        return Calendar.getInstance().get(Calendar.YEAR) - dob.split("/")(2).toInt
    }
}