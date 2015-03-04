package mypipe.util

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

object Eval {

  def apply[A](string: String): A = {
    val toolbox = currentMirror.mkToolBox()
    val tree = toolbox.parse(string)
    toolbox.eval(tree).asInstanceOf[A]
  }
}
