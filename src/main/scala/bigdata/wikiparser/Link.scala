package bigdata.wikiparser

import org.joda.time.DateTime

/**
  * Helper class to store information about one link
  *
  @param pageTitle The title of the page this links belongs to
  @param linkTItle The name of the link (pageTitle of the page pointed to)
  @param count How many time this reference appears in the source page
  @param from First revision when this link (with this count) appears in the source page
  */
class Link ( var pageTitle: String,
                  var linkTitle: String,
                  var count: Int = 0,
                  var from: DateTime = null
                ) extends Serializable {

  def canEqual(other: Any): Boolean = other.isInstanceOf[Link]

  //NOTE: `from` field is not included in equals comparison
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
