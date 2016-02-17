import com.sun.deploy.util.SessionState
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Period}

import scala.collection.mutable.ListBuffer

object Weblog {

  //2015-07-22T09:00:28.069908Z marketpalce-shop 203.200.99.67:41874 10.0.6.195:80 0.000021 0.020869 0.00002 200 200 0 822 "GET https://paytm.com:443/shop/cart?channel=web&version=2 HTTP/1.1" "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2
  val TIME = "(\\d{4}\\-\\d{1,2}\\-\\d{1,2}T\\d{2}:\\d{2}:\\d{2}\\.\\d+)"
  val IP = "(\\d+\\.\\d+\\.\\d+\\.\\d+)"
  val PATH = "([\\/\\w\\?\\=&]+)"
  val BROWSER_SIGNATURE = "(\".+\")"
  val STR_REGEX = TIME + "Z\\s+[\\w\\-]+\\s+" + IP + ":\\d+[\\d:\\.\\s]+\"[A-Z]+\\s[a-z]+:\\/\\/paytm\\.com[:\\d]*" + PATH + "\\sHTTP\\/1\\.1\"\\s+" + BROWSER_SIGNATURE + "\\s+[A-Za-z\\d\\-]+\\s+.+"
  val LINE_REGEX = STR_REGEX.r()
  val EXPIRE_PERIOD = Period.minutes(15)

  def main(args: Array[String]) {
    println("Starting processing the logs...")
    val conf = new SparkConf().setAppName("WeblogChallenge")
    val sc = new SparkContext(conf)
    println("Reading the file with SparkContext")
    val logData = sc.textFile(args(0))

    //Loading each request with relevant data into a list
    val requests = logData
      .map(line => LINE_REGEX.findFirstMatchIn(line))  //transform into a list of matchers
        .filter(lineMatch => lineMatch.isDefined)  //keep non-empty ones
        .map(lineMatch => (lineMatch.get.group(2), List((lineMatch.get.group(1), lineMatch.get.group(3))))) // ip -> time, path

    //Group all the requests by ip (user)
    val usersRequests:RDD[(String, List[(String, String)])] = requests.reduceByKey((l1, l2) => l1 ++ l2)

    //Group users' requests into sessions
    val usersSessions: RDD[(String, List[WebLogSession])] = usersRequests.map(userRequests => groupRequestsIntoSessions(userRequests))

    //Getting a list of averages across users
    val averages = usersSessions.map( userSession => average(userSession._2)).filter( _ > 0)

    println("Average session length is " + (averages.sum()/averages.count()))

    //Determining unique paths per session


    println("Done.")
  }

  def average(sessions: List[WebLogSession]): Double = {
    val lengths = sessions.map(session => session.duration()).filter( _ > 0)
    if (lengths.isEmpty) {
      return 0
    }
    lengths.sum/lengths.size
  }

  def groupRequestsIntoSessions(userRequests: (String, List[(String, String)])): (String, List[WebLogSession]) = {
    val allUserRequests: List[(DateTime, String)] = userRequests._2.sortBy(req => req._1).map( req => (new DateTime(req._1), req._2))

//    println("Requests for user " + userRequests._1)
//    userRequests._2.foreach(req => println(req._1 + ", " + req._2))

    var sessionStartTime: DateTime = null
    var sessionExpireTime: DateTime = null
    var currentSession: WebLogSession = null
    val userSessions: ListBuffer[WebLogSession] = new ListBuffer[WebLogSession]

    for(request <- allUserRequests) {
      if (sessionStartTime == null || request._1.isAfter(sessionExpireTime)) {
        sessionStartTime = request._1
        sessionExpireTime = sessionStartTime.plus(EXPIRE_PERIOD)
        if (currentSession != null) {
          userSessions += currentSession
        }
        currentSession = new WebLogSession(new ListBuffer[(DateTime, String)])
        currentSession.addRequest(request)
      } else {
        currentSession.addRequest(request)
      }
    }

    if (currentSession != null) {
      userSessions += currentSession
    }

//    println("Detected " + userSessions.size + " sessions")

    return (userRequests._1, userSessions.toList)
  }

}