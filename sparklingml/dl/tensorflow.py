#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import unicode_literals

from tensorflowonspark.TFCluster import run

from pyspark import keyword_only
from pyspark.ml import Estimator, Model
from pyspark.ml.param import *
from pyspark.ml.param.shared import HasFeaturesCol, HasLabelCol

class TensorFlowSlimEstimator(Estimator, HasFeaturesCol, HasLabelCol):
    """
    >>> tfsle = TensorFlowSlimEstimator()
    >>> tfsle.setFeaturesCol("features")
    >>> tfsle.setLabelCol("labels")
    >>> tfsle.fromSlim("vgg", "vgg_a", "vgg_arg_scope")
    """
    @keyword_only
    def __init__():
        super(TensorFlowSlimEstimator, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    stepsParam = Param(
        Params._dummy(),
        "steps", "maximum number of steps",
        typeConverter=TypeConverters.toInt)

    argScopeParam = Param(
        Params._dummy(),
        "argScope",
        "Defines the argscope for your model."
        "You can use a slim net to configure with fromSlim()",
        typeConverter=TypeConverters.toString)

    netParam = Param(
        Params._dummy(),
        "net", "the net param."
        "This is evaluated as a raw string with eval."
        "You can use a slim net to configure with fromSlim()",
        typeConverter=TypeConverters.toString)

    syncReplicasParam = Param(
        Params._dummy(),
        "syncReplicas", "if we should use the sync optimizer",
        typeConverter=TypeConverters.toBoolean)

    def __init__(self):
        self._cluster_args = None
        self._cluster = None
        self._setDefault(
            stepsParam=1000, epochsParam=10,
            syncReplicasParam=False)

    def _start_training_cluser(self, num_classes):
        """
        Start a TFCluster if one is not running for a given number of classes.
        """
        if self._cluster:
            if self._cluster_params != self._generate_params():
                self._cluster.stop()
            else:
                return self._cluster
        self._cluster_params = self._generate_params()

        sync_replicas_value = self.getSyncReplicas()
        num_steps = self.getSteps()

        def map_fun(args, ctx):
            from tensorflowonspark import TFNode
            import tensorflow as tf
            import importlib

            slim = tf.contrib.slim

            worker_num = ctx.worker_num
            job_name = ctx.job_name
            task_index = ctx.task_index
            cluster_spec = ctx.cluster_spec

            # Delay PS nodes a bit, since workers seem to reserve GPUs more
            # quickly/reliably (w/o conflict)
            if job_name == "ps":
                time.sleep((worker_num + 1) * 5)
                self._cluster = run(sc, map_fun, tf_args)

            # Convert the batch, TODO use Arrow side here (Spark 2.3+)
            def feed_dict(batch):
                featureVecs = []
                labels = []
                for item in batch:
                    featureVecs.append(item[1])
                    labels.append(item[0])
                xs = numpy.array(featureVecs)
                xs = xs.astype(numpy.float32)
                ys = numpy.array(labels)
                ys = ys.astype(numpy.uint)
                return (xs, ys)

            if job_name == "ps":
                server.join()
            elif job_name == "worker":
                # Assigns ops to the local worker by default.
                with tf.device(tf.train.replica_device_setter(
                        worker_device="/job:worker/task:%d" % task_index,
                        cluster=cluster)):
                    with eval(arg_value):
                        net = eval(net_value)
                        total_loss = slim.losses.get_total_loss()
                        optimizer = tf.train.GradientDescentOptimizer(learning_rate)
                        train_op = slim.learning.create_train_op(total_loss, optimizer)
                        logdir = os.mk
                        slim.learning.train(
                            train_op,
                            logdir,
                            master=server.target,
                            is_chief=(FLAGS.task == 0),
                            number_of_steps=num_steps,
                            save_interval_secs=600,
                            sync_optimizer=optimizer if sync_replicas_value else None)


    def _generate_params(self):
        """
        Generate a lit of params.
        """
        return [self.getSteps(), self.getArgScopeParam(), self.getNetParam()]

    def _fit(self, df):
        selected_df = df.select(
            df.col(self.getLabelCol()).cast(IntegerType()),
            self.getFeaturesCol())
        selected_df.cache()
        num_classes = selected_df.agg(countDistinct(self.getLabelCol())).head()[0]
        self._start_training_cluster(num_classes)

    def setSteps(self, value):
        """
        Sets the value of :py:attr:`stepsParam`.
        """
        return self._set(stepsParam=value)

    def getSteps(self, value):
        """
        Gets the maximum number of steps.
        """
        return self.getOrDefault(self.stepsParam)

    def setNet(self, value):
        """
        Sets the value of :py:attr:`netParam`.
        """
        return self._set(netParam=value)

    def getNet(self, value):
        """
        Get code for the net.
        """
        return self.getOrDefault(self.netParam)

    def setArgScope(self, value):
        """
        Sets the value of :py:attr:`argScopeParam`.
        """
        return self._set(argScopeParam=value)

    def getArgScope(self, value):
        """
        Get code for the arg scope.
        """
        return self.getOrDefault(self.argScopeParam)

    def fromSlim(self, package, net, scope):
        """
        Sets :py:attr:`netParam`. and :py:attr:`argScopeParam` for a given
        slim net.
        """
        self.setNet("slim.nets.{0}.{1}(inputs, num_classes=num_classes)".format(package, net))
        self.setArgScope("slim.nets.{0}.{1}()".format(package, scope))


if __name__ == '__main__':
    tfsle = TensorFlowSlimEstimator()
    tfsle.setFeatureCol("features")
    tfsle.setLabelCol("labels")
    tfsle.fromSlim("vgg", "vgg_a", "vgg_arg_scope")
    sc = SparkContext(conf=SparkConf().setAppName("tflowrun"))
    spark = SparkSession.getOrCreate()
    df = spark.createDataFrame(
        [(1.0, 0.2, 1.0), (3.0, 0.2, 2.0), (4.0, 0.2, 2.0), (4.0, 0.5, 2.0)],
        ["score", "junk", "values"])
    from pypsark.ml.feature import VectorAssembler
    assembler = VectorAssembler(inputCols=["score", "junk"], outputCol="feature")
    assembled = assembler.transform(df)
    tfse = TensorFlowSlimEstimator()
    tfse.setFeatureCol("feature")
    tfse.setLabelCol("values")
    tfse.fit(assembled)
