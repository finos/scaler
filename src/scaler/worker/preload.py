import ast
import importlib
import logging
import os
import traceback
from typing import Any, Optional

logger = logging.getLogger(__name__)


class PreloadSpecError(Exception):
    pass


def execute_preload(spec: str) -> None:
    """
    Import and execute the given preload spec in current interpreter.

    Example: 'foo.bar:preload_function("a", 2)'
    """
    module_path, func_name, args, kwargs = _parse_preload_spec(spec)
    logger.info("preloading: %s:%s with args=%s kwargs=%s", module_path, func_name, args, kwargs)

    try:
        module = importlib.import_module(module_path)
    except ImportError:
        if module_path.endswith(".py") and os.path.exists(module_path):
            raise PreloadSpecError(
                f"Failed to find module. Did you mean '{module_path.rsplit('.', 1)[0]}:{func_name}'?"
            )
        raise

    try:
        target = getattr(module, func_name)
    except AttributeError:
        logger.exception(f"Failed to find attribute {func_name!r} in {module_path!r}.")
        raise PreloadSpecError(f"Failed to find attribute {func_name!r} in {module_path!r}.")

    if not callable(target):
        raise PreloadSpecError("Preload target must be callable.")

    try:
        if args is None:
            # Simple name: call with no args
            target()
        else:
            target(*args, **(kwargs or {}))
    except TypeError as e:
        raise PreloadSpecError("".join(traceback.format_exception_only(TypeError, e)).strip())


def _parse_preload_spec(spec: str) -> tuple[str, str, Optional[list[Any]], Optional[dict[str, Any]]]:
    """
    Parse 'pkg.mod:func(arg1, kw=val)' using AST.
    Returns (module_path, func_name, args_or_None, kwargs_or_None).
    If expression is a simple name (no args), returns args=None, kwargs=None.
    """
    if ":" not in spec:
        raise PreloadSpecError("preload must be in 'module.sub:func(...)' format")

    module_part, obj_expr = spec.split(":", 1)

    # Parse the right-hand side as a single expression
    try:
        expression = ast.parse(obj_expr, mode="eval").body
    except SyntaxError:
        raise PreloadSpecError(f"Failed to parse {obj_expr!r} as an attribute name or function call.")

    if isinstance(expression, ast.Name):
        func_name = expression.id
        args = None
        kwargs = None
    elif isinstance(expression, ast.Call):
        # Ensure the function name is an attribute name only (no dotted path)
        if not isinstance(expression.func, ast.Name):
            raise PreloadSpecError(f"Function reference must be a simple name: {obj_expr!r}")
        func_name = expression.func.id
        try:
            args = [ast.literal_eval(arg) for arg in expression.args]
            kwargs = {kw.arg: ast.literal_eval(kw.value) for kw in expression.keywords}
        except ValueError:
            raise PreloadSpecError(f"Failed to parse arguments as literal values: {obj_expr!r}")
    else:
        raise PreloadSpecError(f"Failed to parse {obj_expr!r} as an attribute name or function call.")

    return module_part, func_name, args, kwargs
