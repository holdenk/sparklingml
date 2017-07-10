/**
 * Base classes to allow Scala pipeline stages to be backed
 * by Python code.
 */

// By default most Python transformers don't support Jython evaluation.
// Mix this trait in to allow the user to request Jython evaluation.
// Experimental.
trait EnableJython {
}

class PythonTransformer {
  // Python function string.
  val pythonFunctionString: String = _
  val pythonFunctionName: String = _
  override def transform(input: DataFrame): DataFrame {
    val pickled_command = gateway.
  }
}
