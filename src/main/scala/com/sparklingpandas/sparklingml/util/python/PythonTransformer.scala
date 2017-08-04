/**
 * Base classes to allow Scala pipeline stages to be backed
 * by Python code.
 */
package com.sparklingpandas.sparklingml.util.python

import com.sparklingpandas.sparklingml.param.HasInputCol

import org.apache.spark.sql._
import org.apache.spark.ml.Transformer

trait PythonTransformer extends Transformer with HasInputCol {
  // Name of the python function to register as a UDF
  val pythonFunctionName: String

  override def transform(input: DataFrame): DataFrame = {
    val registrationProvider = PythonRegistration.pythonRegistrationProvider
    val session = input.sparkSession
    val udf = registrationProvider.registerFunction(
      session,
      pythonFunctionName,
      miniSerializeParams())
  }

  /**
   * Do you need to pass some of your parameters to Python?
   * Put them in here and have them get evaluated with a lambda.
   * I know its kind of sketchy -- sorry!
   */
  def miniSerializeParams(): String = {
    ""
  }
}
