/**
 * Initialize SparklingML on a given environment. This is done to allow
 * SparklingML to setup Python callbacks and does not need to be called
 * in the Python side. This is a little ugly, improvements are especially
 * welcome.
 */

object PythonInitialized {
  // The way how training works its done one stage at a time so no stress.
  var ready = false;
  var gatewayServer: Option[GatewayServer]
  def setup(): Unit = {
    if (!ready) {
      ready = true
      // This is going to get us in trouble some day.
     }
  }
}
