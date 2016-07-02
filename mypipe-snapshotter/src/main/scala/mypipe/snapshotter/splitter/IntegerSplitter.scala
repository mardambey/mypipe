package mypipe.snapshotter.splitter

import mypipe.api.data.ColumnMetadata
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

/** A splitter with INT24 type boundaries (ie: Int).
 *
 *  Based off of Apache Sqoop's equivalent code.
 */
object IntegerSplitter extends Splitter[Int] {

  val log = LoggerFactory.getLogger(getClass)

  override def split(splitByCol: ColumnMetadata, minValue: Option[Int], maxValue: Option[Int]): List[InputSplit] = {
    val lowClausePrefix = splitByCol.name + " >= "
    val highClausePrefix = splitByCol.name + " < "

    val numSplits = 5 // TODO: pass this in, must be >= 1
    val splitLimit = 100 // TODO: get this from config

    if (minValue.isEmpty && maxValue.isEmpty) {
      // Range is null to null. Return a null split accordingly.
      log.info(s"Split range is NULL to NULL for column ${splitByCol.name}")
      List(InputSplit(s"${splitByCol.name} IS NULL", s"${splitByCol.name} IS NULL"))
    } else {

      // Get all the split points together.
      val splitPoints = split2(numSplits, splitLimit, minValue.getOrElse(0), maxValue.getOrElse(0)).toIndexedSeq // TODO: is the getOrElse(0) OK?

      log.info(s"Splits: [$minValue to $maxValue] into $numSplits parts")

      splitPoints foreach { point ⇒
        log.debug(point.toString)
      }

      val splits = new ListBuffer[InputSplit]()

      // Turn the split points into a set of intervals.
      var start = splitPoints.head

      (1 until splitPoints.length) foreach { i ⇒
        val end = splitPoints(i)

        if (i == splitPoints.length - 1) {
          // This is the last one use a closed interval.
          splits += InputSplit(
            lowClausePrefix + start,
            splitByCol.name + " <= " + end
          )
        } else {
          // Normal open-interval case.
          splits += InputSplit(
            lowClausePrefix + start,
            highClausePrefix + end
          )
        }

        start = end
      }

      // At least one extrema is null; add a null split.
      if (minValue.isEmpty || maxValue.isEmpty) {
        splits += InputSplit(s"${splitByCol.name} IS NULL", s"${splitByCol.name} IS NULL")
      }

      splits.toList
    }
  }

  /** Returns a list of longs one element longer than the list of input splits.
   *  This represents the boundaries between input splits.
   *  All splits are open on the top end, except the last one.
   *
   *  So the list [0, 5, 8, 12, 18] would represent splits capturing the
   *  intervals:
   *
   *  [0, 5)
   *  [5, 8)
   *  [8, 12)
   *  [12, 18] note the closed interval for the last split.
   *
   *  @param numSplits Number of split chunks.
   *  @param splitLimit Limit the split size.
   *  @param minVal Minimum value of the set to split.
   *  @param maxVal Maximum value of the set to split.
   *  @return Split values inside the set.
   */
  def split2(numSplits: Int, splitLimit: Int, minVal: Int, maxVal: Int): List[Int] = {

    val splits = new ListBuffer[Int]()

    // We take the min-max interval and divide by the numSplits and also
    // calculate a remainder.  Because of integer division rules, numsplits *
    // splitSize + minVal will always be <= maxVal.  We then use the remainder
    // and add 1 if the current split index is less than the < the remainder.
    // This is guaranteed to add up to remainder and not surpass the value.
    val splitSize = (maxVal - minVal) / numSplits
    val splitSizeDouble = (maxVal.toDouble - minVal.toDouble) / numSplits.toDouble

    if (splitLimit > 0 && splitSizeDouble > splitLimit) {
      // If split size is greater than limit then do the same thing with larger
      // amount of splits.
      log.debug("Adjusting split size {} because it's greater than limit {}", splitSize, splitLimit)
      val newSplits = (maxVal - minVal) / splitLimit
      split2(if (newSplits != numSplits) newSplits else newSplits + 1, splitLimit, minVal, maxVal)

    } else {
      log.info(s"Split size: $splitSize Num splits: $numSplits from: $minVal to: $maxVal")

      val remainder = (maxVal - minVal) % numSplits
      var curVal = minVal

      // This will honor numSplits as long as split size > 0.  If split size is
      // 0, it will have remainder splits.

      val v = 0L to numSplits

        @tailrec def inc(v: Seq[Long]): Unit = {
          val i = v.head
          splits += curVal
          if (curVal >= maxVal || v.tail.isEmpty) {
            // do nothing
          } else {
            curVal += splitSize
            curVal += (if (i < remainder) 1 else 0)
            inc(v.tail)
          }
        }

      inc(v)

      if (splits.length == 1) {
        // make a valid singleton split
        splits += maxVal
      } else if ((maxVal - minVal) <= numSplits) {
        // Edge case when there is lesser split points (intervals) then
        // requested number of splits. In such case we are creating last split
        // with two values, for example interval [1, 5] broken down into 5
        // splits will create following conditions:
        //  * 1 <= x < 2
        //  * 2 <= x < 3
        //  * 3 <= x < 4
        //  * 4 <= x <= 5
        // Notice that the last split have twice more data than others. In
        // those cases we add one maxVal at the end to create following splits
        // instead:
        //  * 1 <= x < 2
        //  * 2 <= x < 3
        //  * 3 <= x < 4
        //  * 4 <= x < 5
        //  * 5 <= x <= 5
        splits += maxVal
      }

      splits.toList
    }
  }
}
