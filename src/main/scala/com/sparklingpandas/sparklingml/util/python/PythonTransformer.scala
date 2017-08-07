/**
 * Base classes to allow Scala pipeline stages to be backed
 * by Python code.
 */
package com.sparklingpandas.sparklingml.util.python

import com.sparklingpandas.sparklingml.param.{HasInputCol, HasOutputCol}

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.ml.Transformer

trait PythonTransformer extends Transformer with HasInputCol with HasOutputCol {
  // Name of the python function to register as a UDF
  val pythonFunctionName: String

  def constructUDF(session: SparkSession) = {
    PythonRegistration.pythonRegistrationProvider match {
      case Some(registrationProvider) =>
        registrationProvider.registerFunction(
          session.sparkContext,
          session,
          pythonFunctionName,
          miniSerializeParams())
      case None =>
        throw new Exception("Registration provider does not exist")
    }
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val session = dataset.sparkSession
    val transformUDF = constructUDF(session)
    dataset.withColumn($(outputCol), transformUDF(dataset($(inputCol))))
  }

  /**
   * Returns the data type of the output column.
   */
  protected def outputDataType: DataType

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    validateInputType(inputType)
    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")
    }
    val outputFields = schema.fields :+
      StructField($(outputCol), outputDataType, nullable = false)
    StructType(outputFields)
  }


  /**
   * Validates the input type. Throw an exception if it is invalid.
   */
  protected def validateInputType(inputType: DataType): Unit

  /**
   * Do you need to pass some of your parameters to Python?
   * Put them in here and have them get evaluated with a lambda.
   * I know its kind of sketchy -- sorry!
   */
  def miniSerializeParams(): String = {
    ""
  }
}
