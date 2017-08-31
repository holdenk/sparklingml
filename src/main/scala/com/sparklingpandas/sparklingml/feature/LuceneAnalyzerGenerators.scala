package com.sparklingpandas.sparklingml.feature

import java.lang.reflect.Modifier
import java.io.PrintWriter

import scala.collection.JavaConverters._
import scala.collection.mutable.StringBuilder

import com.sparklingpandas.sparklingml.CodeGenerator

import org.reflections.Reflections


import org.apache.spark.annotation.DeveloperApi

import org.apache.lucene.analysis.{Analyzer, CharArraySet}


/**
 * Code generator for LuceneAnalyzers (LuceneAnalyzers.scala). Run with
 * {{{
 *   build/sbt "test:runMain com.sparklingpandas.sparklingml.feature.LuceneAnalyzerGenerators"
 * }}}
 */
private[sparklingpandas] object LuceneAnalyzerGenerators extends CodeGenerator {

  def main(args: Array[String]): Unit = {
    val (testCode, transformerCode, pyCode) = generate()
    val testCodeFile = s"${testRoot}/feature/LuceneAnalyzersTests.scala"
    val transformerCodeFile = s"${mainRoot}/feature/LuceneAnalyzers.scala"
    val pyCodeFile = s"${pythonRoot}/feature/lucene_analyzers.py"

    val header = s"""${scalaLicenseHeader}
        |
        |package com.sparklingpandas.sparklingml.feature
        |
        |import org.apache.spark.ml.param._
        |import org.apache.spark.ml.util.Identifiable
        |
        |import org.apache.lucene.analysis.Analyzer
        |
        |import com.sparklingpandas.sparklingml.param._
        |
        |// DO NOT MODIFY THIS FILE!
        |// It was auto generated by LuceneAnalyzerGenerators.
        |
    """.stripMargin('|')
    List((testCode, testCodeFile), (transformerCode, transformerCodeFile)).foreach {
      case (code: String, file: String) =>
        val writer = new PrintWriter(file)
        writer.write(header)
        writer.write(code)
        writer.close()
    }

    val pythonHeader = s"""${pythonLicenseHeader}
        |
        |# DO NOT MODIFY THIS FILE!
        |# It was auto generated by LuceneanalyzerGenerators
        |
        |from __future__ import unicode_literals
        |
        |from pyspark import keyword_only
        |from pyspark.ml.param import *
        |from pyspark.ml.param.shared import HasInputCol, HasOutputCol
        |# The shared params aren't really intended to be public currently..
        |from pyspark.ml.param.shared import *
        |from pyspark.ml.util import *
        |
        |from sparklingml.java_wrapper_ml import *
        |from sparklingml.param.shared import HasStopwords, HasStopwordCase
        |
        |""".stripMargin('|')
    val writer = new PrintWriter(pyCodeFile)
    writer.write(pythonHeader)
    writer.write(pyCode)
    writer.write(pythonDoctestFooter)
    writer.close()
  }

  def generate(): (String, String, String) = {
    val reflections = new Reflections("org.apache.lucene");
    val generalAnalyzers =
      reflections.getSubTypesOf(classOf[org.apache.lucene.analysis.Analyzer])
        .asScala.toList.sortBy(_.toString)
    val concreteAnalyzers =
      generalAnalyzers.filter(cls => !Modifier.isAbstract(cls.getModifiers))
    // A bit of a hack but strip out the factories and such
    val relevantAnalyzers = concreteAnalyzers.filter(cls =>
      !(cls.toString.contains("$") || cls.toString.contains("Factory")))
    val generated = relevantAnalyzers.map{ cls =>
      generateForClass(cls)
    }
    val testCode = new StringBuilder()
    val transformerCode = new StringBuilder()
    val pyCode = new StringBuilder()
    generated.foreach{case (test, transform, python) =>
      testCode ++= test
      transformerCode ++= transform
      pyCode ++= python
    }
    (testCode.toString, transformerCode.toString, pyCode.toString)
  }

  def generateForClass(cls: Class[_]): (String, String, String) = {
    import scala.reflect.runtime.universe._
    val rm = scala.reflect.runtime.currentMirror

    val clsSymbol = rm.classSymbol(cls)
    val clsType = clsSymbol.toType
    val clsFullName = clsSymbol.fullName
    val clsShortName = clsSymbol.name.toString
    val constructors = clsType.members.collect{
      case m: MethodSymbol if m.isConstructor && m.isPublic => m }
    // Once we have the debug version constructorParametersLists should be useful
    val constructorParametersLists = constructors.map(_.paramLists).toList
    val constructorParametersSizes = constructorParametersLists.map(_(0).size)
    val javaReflectionConstructors = cls.getConstructors().toList
    val publicJavaReflectionConstructors =
      javaReflectionConstructors.filter(cls => Modifier.isPublic(cls.getModifiers()))
    val constructorParameterTypes = publicJavaReflectionConstructors.map(_.getParameterTypes())
    // We do this in Java as well since some of the scala reflection magic returns private
    // constructors even though its filtered for public. See CustomAnalyzer for an example.
    val javaConstructorParametersSizes = constructorParameterTypes.map(_.size)
    // Since this isn't built with -parameters by default :(
    // we'd need a local version built with it to auto generate
    // the code here with the right parameters.
    // https://docs.oracle.com/javase/tutorial/reflect/member/methodparameterreflection.html
    // For now we could dump the class names and go from their
    // or we could play a game of pin the field on the constructor.
    // local build sounds like the best plan, lets do that l8r

    // Special case for handling stopword analyzers
    val baseClasses = clsType.baseClasses
    // Normally we'd do a checks with <:< but the Lucene types have multiple
    // StopwordAnalyzerBase's that don't inherit from eachother.
    val isStopWordAnalyzer = baseClasses.exists(_.asClass.fullName.contains("Stopword"))

    val charsetConstructors =
      constructorParameterTypes.filter(! _.exists(_ != classOf[CharArraySet]))
    val charsetConstructorSizes = charsetConstructors.map(_.size)

    // If it is a stop word analyzer and has a constructor with two charsets then it takes
    // the stopwords as a parameter.
    if (isStopWordAnalyzer && charsetConstructorSizes.contains(1)) {
      // If there are more parameters
      val includeWarning = constructorParametersSizes.exists(_ > 1)
      val warning = if (includeWarning) {
        s"""
         | * There are additional parameters which can not yet be contro
lled through this API
         | * See https://github.com/sparklingpandas/sparklingml/issues/3"""
          .stripMargin('|')
      } else {
        ""
      }
      val testCode =
        s"""
        |/**
        | * A super simple test
        | */
        |class ${clsShortName}LuceneTest
        |    extends LuceneStopwordTransformerTest[${clsShortName}Lucene] {
        |    val transformer = new ${clsShortName}Lucene()
        |}
        |""".stripMargin('|')
      val code =
        s"""
        |/**
        | * A basic Transformer based on ${clsShortName}.
        | * Supports configuring stopwords.${warning}
        | */
        |
        |class ${clsShortName}Lucene(override val uid: String)
        |    extends LuceneTransformer[${clsShortName}Lucene]
        |    with HasStopwords with HasStopwordCase {
        |
        |  def this() = this(Identifiable.randomUID("${clsShortName}"))
        |
        |  def buildAnalyzer(): Analyzer = {
        |    // In the future we can use getDefaultStopWords here to allow people
        |    // to control the snowball stemmer distinctly from the stopwords.
        |    // but that is a TODO for later.
        |    if (isSet(stopwords)) {
        |      new ${clsFullName}(
        |        LuceneHelpers.wordstoCharArraySet($$(stopwords), !$$(stopwordCase)))
        |    } else {
        |      new ${clsFullName}()
        |    }
        |  }
        |}
        |""".stripMargin('|')
      val pyCode =
        s"""
        |class ${clsShortName}Lucene(
        |        SparklingJavaTransformer, HasInputCol, HasOutputCol,
        |        HasStopwords, HasStopwordCase):
        |    \"\"\"
        |    >>> from pyspark.sql import SparkSession
        |    >>> spark = SparkSession.builder.master("local[2]").getOrCreate()
        |    >>> df = spark.createDataFrame([("hi boo",), ("bye boo",)], ["vals"])
        |    >>> transformer = ${clsShortName}Lucene()
        |    >>> transformer.setParams(inputCol="vals", outputCol="out")
        |    ${clsShortName}Lucene_...
        |    >>> result = transformer.transform(df)
        |    >>> result.count()
        |    2
        |    >>> transformer.setStopwordCase(True)
        |    ${clsShortName}Lucene_...
        |    >>> result = transformer.transform(df)
        |    >>> result.count()
        |    2
        |    \"\"\"
        |    package_name = "com.sparklingpandas.sparklingml.feature"
        |    class_name = "${clsShortName}Lucene"
        |    transformer_name = package_name + "." + class_name
        |
        |    @keyword_only
        |    def __init__(self, inputCol=None, outputCol=None,
        |                 stopwords=None, stopwordCase=False):
        |        \"\"\"
        |        __init__(self, inputCol=None, outputCol=None,
        |                 stopwords=None, stopwordCase=False)
        |        \"\"\"
        |        super(${clsShortName}Lucene, self).__init__()
        |        self._setDefault(stopwordCase=False)
        |        kwargs = self._input_kwargs
        |        self.setParams(**kwargs)
        |
        |    @keyword_only
        |    def setParams(self, inputCol=None, outputCol=None,
        |                  stopwords=None, stopwordCase=False):
        |        \"\"\"
        |        setParams(inputCol=None, outputCol=None,
        |                  stopwords=None, stopwordCase=False)
        |        \"\"\"
        |        kwargs = self._input_kwargs
        |        return self._set(**kwargs)
        |
        |""".stripMargin('|')
      (testCode, code, pyCode)
    } else if (constructorParametersSizes.contains(0) &&
      javaConstructorParametersSizes.contains(0)) {
      val testCode =
        s"""
        |/**
        | * A super simple test
        | */
        |class ${clsShortName}LuceneTest
        |    extends LuceneTransformerTest[${clsShortName}Lucene] {
        |    val transformer = new ${clsShortName}Lucene()
        |}
        |""".stripMargin('|')
      val code =
        s"""
        |/**
        | * A basic Transformer based on ${clsShortName} - does not support
        | * any configuration properties.
        | * See https://github.com/sparklingpandas/sparklingml/issues/3
        | * & LuceneAnalyzerGenerators for details.
        | */
        |
        |class ${clsShortName}Lucene(override val uid: String)
        |    extends LuceneTransformer[${clsShortName}Lucene] {
        |
        |  def this() = this(Identifiable.randomUID("${clsShortName}"))
        |
        |  def buildAnalyzer(): Analyzer = {
        |    new ${clsFullName}()
        |  }
        |}
        |""".stripMargin('|')
      (testCode, code, "")
    } else {
      ("", s"""
        |/* There is no default zero arg constructor for
        | *${clsFullName}.
        | */
        |""".stripMargin('|'), "")
    }
  }
}
