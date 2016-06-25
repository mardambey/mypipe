package mypipe.snapshotter.splitter

import mypipe.api.data.ColumnMetadata

trait Splitter[T] {
  def split(splitByCol: ColumnMetadata, minValue: T, maxValue: T): List[InputSplit]
}

