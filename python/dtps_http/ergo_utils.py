from functools import wraps
from typing import Type

__all__ = [
    "allow_exceptions",
    "reapply_decorators",
]


def allow_exceptions(*allowed_exceptions: Type[Exception]):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except allowed_exceptions as e:
                # Re-raise the allowed exception
                raise e
            except Exception as e:
                # Handle disallowed exceptions
                msg = (
                    f"This function only allows the exceptions: {allowed_exceptions} but got "
                    f"{e.__class__.__name__}"
                )
                raise RuntimeError(msg) from e

        wrapper._allowed_exceptions = allowed_exceptions  # type: ignore
        return wrapper

    return decorator


def reapply_decorators(cls):
    for b in cls.__bases__:
        for attr_name, attr_value in cls.__dict__.items():
            if callable(attr_value):
                base_method = getattr(b, attr_name, None)
                if base_method and hasattr(base_method, "_allowed_exceptions"):
                    allowed_exceptions = base_method._allowed_exceptions
                    setattr(cls, attr_name, allow_exceptions(*allowed_exceptions)(attr_value))
    return cls
