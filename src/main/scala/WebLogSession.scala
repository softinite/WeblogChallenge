import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer

/**
  * Represents a session
  * Created by Softinite on 2016-02-16.
  */
case class WebLogSession(requests : ListBuffer[(DateTime, String)]) {
  def uniquePaths(): Set[String] = {
    requests.map(req => req._2).toSet
  }

  def duration(): Double = {
    if (requests.size <= 1) {
      return 0
    }
    return requests.last._1.getMillis - requests.head._1.getMillis
  }

  def addRequest(request: (DateTime, String)) = {
    requests += request
  }

}
