/**
 * Initialize SparklingML on a given environment. This is done to allow
 * SparklingML to setup Python callbacks and does not need to be called
 * in the Python side. This is a little ugly, improvements are especially
 * welcome.
 */

// Idea: implement a Python class to do serialization
// It evaluates functionString then does wrap on function name
// and returns the Python function.
// This can run in a seperate Python instance and seperate call back env
trait PythonRegisterationProvider {
  def registerFunction(functionString, functionName): PythonFunction
}


object PythonRegistration {
  val gatewayServer: GatewayServer = {
  }
  val pythonRegistrationProvider: PythonRegisterProvider = {
  }
}
