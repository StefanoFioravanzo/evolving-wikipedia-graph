package bigdata.wikiparser

import org.joda.time.DateTime

class Link ( var pageTitle: String,
                  var linkTitle: String,
                  var count: Int = 0,
                  var from: DateTime = null
                ) extends Serializable {

  //NOTE: from field is not included in equal comparison!

  def canEqual(other: Any): Boolean = other.isInstanceOf[Link]

  override def equals(other: Any): Boolean = other match {
    case that: Link =>
      (that canEqual this) &&
        pageTitle == that.pageTitle &&
        linkTitle == that.linkTitle &&
        count == that.count
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(pageTitle, linkTitle, count)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
