/**
 * A wrapper for Intel's Big DL estimator to allow it to be used
 * in a traditional Spark ML pipeline.
 */

import org.apache.spark.ml._

import  com.intel.analytics.bigdl._

class IntelBigDLEstimator extends Estimator[IntelBigDLClassifier] {
}
