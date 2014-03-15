package mypipe.mysql

class HostPortUserPass(val host: String, val port: Int, val user: String, val password: String)

object HostPortUserPass {

  def apply(hostPortUserPass: String) = {
    val params = hostPortUserPass.split(":")
    new HostPortUserPass(params(0), params(1).toInt, params(2), params(3))
  }
}