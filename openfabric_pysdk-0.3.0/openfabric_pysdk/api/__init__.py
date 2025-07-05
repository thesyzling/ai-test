from openfabric_pysdk.utility import LoaderUtil

# Execution callback function
execution_callback_function = LoaderUtil.get_function("main_callback")
execution_callback_function_params = LoaderUtil.get_function_params("main_callback")

# Config callback function
config_callback_function = LoaderUtil.get_function("config_callback")
config_callback_function_params = LoaderUtil.get_function_params("config_callback")

# Config callback function
suspend_callback_function = LoaderUtil.get_function("suspend_callback")
suspend_callback_function_params = LoaderUtil.get_function_params("suspend_callback")

# Config callback function
cancel_callback_function = LoaderUtil.get_function("cancel_callback")
cancel_callback_function_params = LoaderUtil.get_function_params("cancel_callback")
