package mypipe.util

import org.slf4j.LoggerFactory

object Lists {

  protected val log = LoggerFactory.getLogger(getClass)

  def processList[T](list: List[T],
                     listOp: (T) ⇒ Boolean,
                     onError: (List[T], T) ⇒ Boolean): Boolean = {

    list.forall(item ⇒ {
      val res = try { listOp(item) } catch {
        case e: Exception ⇒
          log.error("Unhandled exception while processing list", e)
          onError(list, item)
      }

      if (!res) {
        // fail-fast if the error handler returns false
        onError(list, item)
      } else true

    })
  }

}
