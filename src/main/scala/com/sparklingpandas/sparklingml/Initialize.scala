/**
 * Initialize SparklingML on a given environment. This is done to allow
 * SparklingML to setup Python callbacks and does not need to be called
 * in the Python side. This is a little ugly, improvements are especially
 * welcome.
 */

object PythonInitialized {
  def setup(): Unit = {
    // This is going to get us in trouble some day.
    import org.apache.spark.deploy.PythonRunner
    PythonRunner.main(Array("python/sparklingml/udf_registration.py", ""))
  }
}
