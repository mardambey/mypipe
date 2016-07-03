package mypipe.snapshotter.splitter

import mypipe.api.data.ColumnMetadata

case class BoundingValues[T](lowerBound: Option[T], upperBound: Option[T])

trait Splitter[T] {
  def split(splitByCol: ColumnMetadata, boundingValues: BoundingValues[T], numSplits: Int, splitLimit: Int): List[InputSplit]
}

