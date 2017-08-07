from py4j.java_gateway import JavaGateway, CallbackServerParameters
from .transformation_functions import *

# This class is used to allow the Scala process to call into Python
# It may not run in the same Python process as your regular Python
# shell if you are running PySpark normally.
class PythonRegistrationProvider(object):

    def __init__(self, gateway):
        self.gateway = gateway
        self._sc = None
        self._session = None

    def registerFunction(self, jsc, jsession, functionName, params):
        master = sc.master
        if not self._sc:
            self._sc = SparkContext(master=master, gateway=self.gateway, jsc=jsc)
        if not self._session:
            self._session = SparkSession.builder.getOrCreate()
        session = self._session
        if functions_info has functionName:
            function_info = functions_info[functionName]
            func = function_info.func(eval(params))
            retType = func_info.type
            session.udf().register(function_name, func, retType)
        else:
            print("Could not find function")
            return None
        return "A Return Value"

    class Java:
        implements = ["com.sparklingpandas.sparklingml.util.python.PythonRegisterationProvider"]

if __name__ == "__main__":
    gateway = JavaGateway(
        callback_server_parameters=CallbackServerParameters())
    provider = PythonRegistrationProvider(gateway)
    gateway.entry_point.registerListener(provider)
