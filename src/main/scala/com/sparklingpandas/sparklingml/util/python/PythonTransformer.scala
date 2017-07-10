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
  // Name of your UDF, your UDF should have performed its registration
  // in udf_registration.py
  val udfName: String = _

  override def transform(input: DataFrame): DataFrame {
  }
}
