import inspect
from typing import Any, Callable, Dict, Set, Tuple, Type
from unittest.mock import Mock


COLOR_CODES = {
    "blue": "\033[94m",
    "green": "\033[92m",
    "purple": "\033[95m",
    "red": "\033[91m",
    "yellow": "\033[93m",
}


END_CODE = "\033[0m"


def red(s: str) -> str:
    """Format the input string as red.

    Args:
        s: The input string.

    """
    return COLOR_CODES["red"] + str(s) + END_CODE


def green(s: str) -> str:
    """Format the input string as green.

    Args:
        s: The input string.

    """
    return COLOR_CODES["green"] + str(s) + END_CODE


def blue(s: str) -> str:
    """Format the input string as blue.

    Args:
        s: The input string.

    """
    return COLOR_CODES["blue"] + str(s) + END_CODE


def purple(s: str) -> str:
    """Format the input string as purple.

    Args:
        s: The input string.

    """
    return COLOR_CODES["purple"] + str(s) + END_CODE


def yellow(s: str) -> str:
    """Format the input string as purple.

    Args:
        s: The input string.

    """
    return COLOR_CODES["yellow"] + str(s) + END_CODE


def _render(
    obj: object,
    level: int = 0,
    indent: str = "    ",
    treelike_whitelist: Tuple[type, ...] = None,
    treelike_blacklist: Tuple[type, ...] = None,
    attr_blacklist: Tuple[str] = None,
    renderer_overrides: Dict[Type, Callable] = None,
) -> str:
    """Helper function for :py:func:`.render` to render arguments to an object.

    Args:
        obj: The object to render.
        level: The level in the tree at which the `obj` is rendered.
        indent: What represents a single indentation.
        treelike_whitelist: Classes to render in a tree-like fashion.
        attr_blacklist: attributes to exclude
        renderer_overrides: a mapping from classes to functions that should be used
            to render those classes. The function should take an instance of the class
            and return the rendered string

    Returns:
        A string representation of the object formatted as a tree.

    """
    if renderer_overrides is None:
        renderer_overrides = {}

    next_level = level + 1
    if treelike_blacklist and isinstance(obj, treelike_blacklist):
        argstr = blue(repr(type(obj)))
    elif treelike_whitelist and isinstance(obj, treelike_whitelist):
        argstr = render(
            obj,
            level=level,
            treelike_whitelist=treelike_whitelist,
            treelike_blacklist=treelike_blacklist,
            attr_blacklist=attr_blacklist,
            renderer_overrides=renderer_overrides,
        )
    elif isinstance(obj, str):
        argstr = green('"{}"'.format(obj))
    elif isinstance(obj, list):
        if len(obj) > 0:
            sub_elements = [
                render(
                    el,
                    next_level,
                    treelike_whitelist=treelike_whitelist,
                    treelike_blacklist=treelike_blacklist,
                    attr_blacklist=attr_blacklist,
                    renderer_overrides=renderer_overrides,
                )
                for el in obj
            ]
            argstr = (
                "[\n"
                + (indent * next_level)
                + (",\n" + (indent * next_level)).join(sub_elements)
                + "\n"
                + (indent * level)
                + "]"
            )
        else:
            argstr = "[]"
    elif isinstance(obj, dict):
        if len(obj) > 0:
            key_value_pairs = []
            for key, value in obj.items():
                value_str = render(
                    value,
                    next_level,
                    treelike_whitelist=treelike_whitelist,
                    treelike_blacklist=treelike_blacklist,
                    attr_blacklist=attr_blacklist,
                    renderer_overrides=renderer_overrides,
                )
                key_value_pairs.append(f'{indent * next_level}"{key}"={value_str}\n')
            key_value_str = "".join(key_value_pairs)
            argstr = f"{{\n{key_value_str}{indent * level}}}"
        else:
            argstr = "{}"
    elif type(obj) in renderer_overrides:
        argstr = blue(renderer_overrides[type(obj)](obj))
    else:
        argstr = blue(repr(obj))

    return argstr


def _get_default_arguments(obj: Any) -> Set[str]:
    """Get the names of the default arguments on an object

    The default arguments are determined by comparing the object to one constructed
    from the object's class's initializer with Mocks for all positional arguments

    Arguments:
        obj: the object to find default arguments on

    Returns:
        the set of default arguments
    """
    cls = type(obj)
    obj_sig = inspect.signature(cls.__init__)
    try:
        mocked_obj = cls(
            **{
                param.name: Mock()
                for param in obj_sig.parameters.values()
                if param.default == param.empty
                and param.kind != param.VAR_KEYWORD
                and param.name != "self"
            }
        )
    except Exception:
        return set()
    return {
        key
        for key, value in obj.__dict__.items()
        if key in mocked_obj.__dict__ and mocked_obj.__dict__[key] == value
    }


def render(
    obj: object,
    level: int = 0,
    indent: str = "    ",
    treelike_whitelist: Tuple[type, ...] = None,
    treelike_blacklist: Tuple[type, ...] = None,
    attr_blacklist: Tuple[str] = None,
    renderer_overrides: Dict[type, Callable[[Any], str]] = None,
) -> str:
    """Render any object in tree form.

    Example:

        Class(
            arg1=val1,
            arg2=[
                val2,
                val3
            ]
            arg3=Class(
                arg4=val4
            )

    Args:
        obj: The object to render.
        level: The level in the tree at which the `obj` is rendered.
        indent: What represents a single indentation.
        treelike_whitelist: Classes to render in a tree-like fashion.
        treelike_blacklist: Classes to render with the typename only.
        attr_blacklist: attributes to exclude
        renderer_overrides: a mapping from classes to functions that should be used
            to render those classes. The function should take an instance of the class
            and return the rendered string

    Returns:
        A string representation of the object formatted as a tree.

    """
    next_level = level + 1

    if renderer_overrides is None:
        renderer_overrides = {}

    if not treelike_whitelist or not isinstance(obj, treelike_whitelist):
        return _render(
            obj,
            level=level,
            treelike_whitelist=treelike_whitelist,
            treelike_blacklist=treelike_blacklist,
            attr_blacklist=attr_blacklist,
            renderer_overrides=renderer_overrides,
        )

    result = red(obj.__class__.__name__) + "(\n"

    argnames = sorted(obj.__dict__.keys())
    default_argnames = _get_default_arguments(obj)

    for argname in argnames:
        if argname in default_argnames:
            continue
        if attr_blacklist is not None and argname in attr_blacklist:
            continue
        if argname in {"kwargs", "args"}:
            continue
        if argname in ("override", "unbound_idea"):
            argval = getattr(obj, argname) is not None
        else:
            argval = getattr(obj, argname)
        argstr = "{}={}".format(
            purple(argname),
            _render(
                argval,
                next_level,
                treelike_whitelist=treelike_whitelist,
                treelike_blacklist=treelike_blacklist,
                attr_blacklist=attr_blacklist,
                renderer_overrides=renderer_overrides,
            ),
        )
        delim = "," if argname != argnames[-1] else ""
        result += "{}{}{}\n".format(next_level * indent, argstr, delim)

    result += level * indent + ")"
    return result
