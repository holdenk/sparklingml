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

from tensorflowonspark import run

from pyspark import keyword_only
from pyspark.ml import Model
from pyspark.ml.param import *
from pyspark.ml.param.shared import HasInputCol, HasOutputCol

class TensorFlowSlimEstimator(Model, HashInputCol, HasOutputCol):
    stepsParam = Param(Params._dummy(),
                       "steps", "maximum number of steps", 
                       typeConverter=TypeConverters.toInt)

    epochsParam = Param(Params._dummy(),
                        "epochs", "number of epochs",
                        typeConverter=TypeConverters.toInt)

    rdmaParam = Param(Params._dummy(),
                      "rdma", "use rdma",
                      typeConverter=TypeConverters.toBoolean)

    numGpusParam = Param(Params._dummy(),
                    "gpus", "number of GPUs",
                    typeConverter=TypeConverters.toInt)

    layersParam = Param(Params._dummy(),
                        "layers", "the layers"
                        typeConverter=TypeConverters.toListString)

    optimizerParam = Param(
        Params._dummy()
        'optimizer',
        'The name of the optimizer, one of "adadelta", "adagrad", "adam",'
        '"ftrl", "momentum", "sgd" or "rmsprop".')

    loss
    def __init__(self):
        self._cluster_args = None
        self._cluster = None
        self._setDefault(
            stepsParam=1000, epochsParam=10, rdmaParam=True, numGpusParam=0,
            optimizerParam="ftrl")

    def _start_cluser(self):
        """
        Start a TFCluster if one is not running.
        """
        if self._cluster:
            if self._cluster_params != self._generate_params():
                self._cluster.stop()   
            else:
                return self._cluster
        self._cluster_params = self._generate_params()
        layers_
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

            # Convert the batch
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
                    eval(layers)
                    global_step = slim.variables.global_step()
                    num_batches_per_epoch = (
                        dataset.num_examples_per_epoch() / batch_size)
                    decay_steps = int(
                        num_batches_per_epoch * num_epochs_per_decay /
                        num_replicas_to_aggregate)
                    
                    
    def _generate_params(self):
        """
        Generate a lit of params.
        """
        return [self.getSteps(), self.getEpochs()]

    def fit(self,):
        self._start_cluster()

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

    def setEpoch(self, value):
        """
        Sets the value of :py:attr:`epochParam`.
        """
        return self._set(epochParam=value)

    def getEpoch(self, value):
        """
        Gets the numbers of epoch.
        """
        return self.getOrDefault(self.epochParam)

    def setRdma(self, value):
        """
        Sets the value of :py:attr:`rdmaParam`.
        """
        return self._set(rdmaParam=value)

    def getRdma(self, value):
        """
        Get if rdma is enabled.
        """
        return self.getOrDefault(self.rdmaParam)

    def setBaseNet(self, value):
        """
        Sets the value of :py:attr:`baseNetParam`.
        """
        return self._set(rdmaParam=value)

    def getBaseNet(self, value):
        """
        Get name of the base network.
        """
        return self.getOrDefault(self.rdmaParam)
