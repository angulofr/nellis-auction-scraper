"""
Shared helper loader that safely extracts pure module-level helpers and
`validate_readonly_sql` from `app.py` into an isolated namespace for unit tests
without launching the Streamlit UI script body.
"""

import ast
import types
from pathlib import Path

import duckdb

APP_PATH = Path(__file__).resolve().parents[1] / "app.py"


class _PassThroughSt:
    """Minimal Streamlit stub for isolated helper evaluation in unit tests."""
    __path__ = []

    class _NoopCtx:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def __getattr__(self, _name):
        def _noop(*_args, **_kwargs):
            return self._NoopCtx()
        return _noop

    def cache_data(self, *_args, **_kwargs):
        def _deco(fn):
            fn.clear = lambda: None
            return fn
        return _deco

    def cache_resource(self, *_args, **_kwargs):
        def _deco(fn):
            fn.clear = lambda: None
            return fn
        return _deco

    def dialog(self, *_args, **_kwargs):
        def _deco(fn):
            return fn
        return _deco


def load_app_module_helpers() -> types.ModuleType:
    """
    Parse `app.py` cleanly via AST, executing all imports, constants, pure helper
    functions, and `validate_readonly_sql` while skipping Streamlit UI top-level calls.
    """
    src_text = APP_PATH.read_text(encoding="utf-8")
    tree = ast.parse(src_text, filename=str(APP_PATH))

    ns = types.ModuleType("app_helpers_isolated")
    ns.__dict__["__file__"] = str(APP_PATH)
    in_mem_con = duckdb.connect()

    selected_nodes = []
    validate_sql_node = None

    for node in tree.body:
        # Stop collecting top-level UI before sidebar controls, but inspect SQL Studio for validate_readonly_sql
        if isinstance(node, ast.If):
            src_seg = ast.get_source_segment(src_text, node) or ""
            if "view_mode == \"🎯 Deal Finder\"" in src_seg:
                for sub in ast.walk(node):
                    if isinstance(sub, ast.FunctionDef) and sub.name == "validate_readonly_sql":
                        validate_sql_node = sub
                        break
                break
            continue
        if isinstance(node, (ast.Import, ast.ImportFrom, ast.FunctionDef, ast.ClassDef)):
            selected_nodes.append(node)
        elif isinstance(node, ast.Assign):
            targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
            # Skip eager `con = get_duckdb_con()` initialization and `view_mode = ...`
            if any(t in ("con", "view_mode") for t in targets):
                continue
            selected_nodes.append(node)

    if validate_sql_node is not None:
        selected_nodes.append(validate_sql_node)

    mod_ast = ast.Module(body=selected_nodes, type_ignores=[])
    ast.fix_missing_locations(mod_ast)
    code_obj = compile(mod_ast, filename=str(APP_PATH), mode="exec")

    import sys
    stub = _PassThroughSt()
    stub.components = stub
    stub.v1 = stub
    patched_mods = {
        "streamlit": sys.modules.get("streamlit"),
        "streamlit.components": sys.modules.get("streamlit.components"),
        "streamlit.components.v1": sys.modules.get("streamlit.components.v1"),
    }
    sys.modules["streamlit"] = stub
    sys.modules["streamlit.components"] = stub
    sys.modules["streamlit.components.v1"] = stub
    try:
        exec(code_obj, ns.__dict__)
    finally:
        for k, orig in patched_mods.items():
            if orig is not None:
                sys.modules[k] = orig
            else:
                sys.modules.pop(k, None)

    ns.__dict__["con"] = in_mem_con
    return ns
