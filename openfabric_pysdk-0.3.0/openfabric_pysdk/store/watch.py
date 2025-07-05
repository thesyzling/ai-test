import pprint
from functools import wraps
from typing import Any, Callable, List, Literal, Optional, Tuple

from openfabric_pysdk.logger import logger
from openfabric_pysdk.service import PersistenceService


# ------------------------------------------------------------------------
# Class to handle qid-related operations and decorator logic
class WatchHelper:
    def __init__(self, key: Literal['in', 'out', 'ray'], deserializer: Callable[[Any], Any] = None):
        self.key = key
        self.deserializer = deserializer
        self.last_modified: Optional[float] = None

    # ------------------------------------------------------------------------
    # Search for 'qid' in args/kwargs and return the object and its location
    def __find_qid_and_location(self, obj: Any) -> Tuple[Optional[Any], List[str]]:
        if hasattr(obj, 'qid'):
            return obj, []  # Return object and empty path (no nesting)

        # Recursively check dictionaries
        if isinstance(obj, dict):
            for key, value in obj.items():
                result, path = self.__find_qid_and_location(value)
                if result is not None:
                    return result, ['dict', key] + path

        # Recursively check lists
        if isinstance(obj, list):
            for index, value in enumerate(obj):
                result, path = self.__find_qid_and_location(value)
                if result is not None:
                    return result, ['list', index] + path

        return None, []

    # ------------------------------------------------------------------------
    # Update the object in the correct location
    def __update_nested_object(self, obj: Any, path: List[str], new_data: Any) -> Any:
        if not path:
            return new_data  # If there's no path, replace the whole object

        current = obj
        for step in path[:-2]:  # Traverse to the second-to-last step
            if isinstance(step, str) and step == 'dict':
                current = current[path[1]]
            elif isinstance(step, str) and step == 'list':
                current = current[path[1]]

        # Now, update the last step
        last_step = path[-2]
        last_key = path[-1]
        if last_step == 'dict':
            current[last_key] = new_data
        elif last_step == 'list':
            current[last_key] = new_data

        return obj

    # ------------------------------------------------------------------------
    # Check for file changes and retrieve new data if needed
    def __check_for_updates(self, qid: str):
        current_modified = PersistenceService.get_asset_timestamp(qid, self.key)

        if self.last_modified is None or current_modified > self.last_modified:
            self.last_modified = current_modified
            return PersistenceService.get_asset(qid, self.key, self.deserializer)

        return None

    # ------------------------------------------------------------------------
    # Main logic of the decorator
    def wrap(self, func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Find qid and the location of the object containing it
            obj_with_qid, path_to_qid = None, []

            # Search through args
            for idx, arg in enumerate(args):
                obj_with_qid, path = self.__find_qid_and_location(arg)
                if obj_with_qid:
                    path_to_qid = ['args', idx] + path
                    break

            # If not found in args, search through kwargs
            if not obj_with_qid:
                for key, value in kwargs.items():
                    obj_with_qid, path = self.__find_qid_and_location(value)
                    if obj_with_qid:
                        path_to_qid = ['kwargs', key] + path
                        break

            # Raise an error if no qid was found
            if obj_with_qid is None:
                logger.error("No 'qid' attribute found in function arguments.")
                return func(*args, **kwargs)

            qid = getattr(obj_with_qid, 'qid')

            # Check for file updates and get new data if available
            new_data = self.__check_for_updates(qid)

            if new_data is not None:
                # Update the nested object
                if path_to_qid[0] == 'args':
                    idx = path_to_qid[1]
                    args = list(args)
                    args[idx] = self.__update_nested_object(args[idx], path_to_qid[2:], new_data)
                    logger.debug(f"Updated argument {idx} with new data {pprint.pformat(new_data)}")
                elif path_to_qid[0] == 'kwargs':
                    key = path_to_qid[1]
                    kwargs[key] = self.__update_nested_object(kwargs[key], path_to_qid[2:], new_data)
                    logger.debug(f"Updated keyword argument '{key}' with new data {pprint.pformat(new_data)}")

            # Call the original function with updated arguments
            return func(*args, **kwargs)

        return wrapper


# ------------------------------------------------------------------------
# Decorator function that uses the QIDHelper class
def watch(key: Literal['in', 'out', 'ray'], deserializer: Callable[[Any], Any] = None):
    def decorator(func: Callable):
        helper = WatchHelper(key, deserializer)
        return helper.wrap(func)

    return decorator
