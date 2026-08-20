import pytest

from app import sanitize_code


class TestSanitizeCode:
    @pytest.mark.parametrize(
        "snippet",
        [
            "import subprocess",
            "import socket",
            "import ctypes",
            "import multiprocessing",
            "import threading",
            "import importlib",
        ],
    )
    def test_strips_blocked_imports(self, snippet):
        out = sanitize_code(f"x = 1\n{snippet}\ny = 2")
        assert snippet not in out
        assert "x = 1" in out

    def test_strips_from_import_of_blocked_module(self):
        out = sanitize_code("from subprocess import run\nx = 1")
        assert "subprocess" not in out

    @pytest.mark.parametrize(
        "call",
        ["eval('1+1')", "exec('x=1')", "compile('x', 'f', 'exec')", "__import__('os')"],
    )
    def test_strips_blocked_builtin_calls(self, call):
        out = sanitize_code(f"x = 1\ny = {call}\nz = 2")
        assert call not in out
        assert "x = 1" in out

    @pytest.mark.parametrize(
        "call",
        ["os.system('rm -rf /')", "os.popen('ls')", "sys.exit(1)", "os.kill(1, 9)"],
    )
    def test_strips_blocked_attribute_calls(self, call):
        out = sanitize_code(f"x = 1\n{call}\ny = 2")
        assert call not in out
        assert "x = 1" in out

    def test_strips_builtins_access(self):
        out = sanitize_code("x = 1\ny = builtins.open\nz = 2")
        assert "builtins" not in out

    def test_preserves_safe_code(self):
        code = "body = []\nbody.append(bp('Texto'))\ntotal = sum([1, 2, 3])"
        out = sanitize_code(code)
        assert "body.append(bp('Texto'))" in out
        assert "sum([1, 2, 3])" in out

    def test_survives_syntax_errors(self):
        # AST parsing fails, so pattern matching alone applies; must not raise.
        out = sanitize_code("def broken(:\n    pass")
        assert isinstance(out, str)

    def test_strips_helper_redefinitions(self):
        code = "def esc(t):\n    return t\nbody = []"
        out = sanitize_code(code)
        assert "def esc" not in out
        assert "body = []" in out

    def test_helper_redefinition_body_also_removed(self):
        code = "def run(text):\n    return 'hacked'\n\nx = 1"
        out = sanitize_code(code)
        assert "hacked" not in out
        assert "x = 1" in out

    def test_keeps_non_helper_function_definitions(self):
        out = sanitize_code("def calcular_total(v):\n    return v * 2")
        assert "def calcular_total" in out

    def test_strips_redundant_imports(self):
        out = sanitize_code("import os\nimport json\nbody = []")
        assert "import os" not in out
        assert "import json" not in out
        assert "body = []" in out

    def test_strips_datetime_import(self):
        out = sanitize_code("from datetime import date\nx = 1")
        assert "from datetime import" not in out

    def test_removes_astral_plane_characters(self):
        out = sanitize_code("x = 1  # 🎉")
        assert "🎉" not in out

    def test_normalizes_smart_quotes(self):
        out = sanitize_code("x = ‘a’")
        assert "‘" not in out and "’" not in out
        assert "'a'" in out

    def test_normalizes_smart_double_quotes(self):
        out = sanitize_code("x = “a”")
        assert "“" not in out and "”" not in out

    def test_normalizes_checkmarks_and_dashes(self):
        out = sanitize_code("# ✓ • — …")
        assert "✓" not in out
        assert "•" not in out
        assert "—" not in out
        assert "…" not in out

    def test_replaces_non_breaking_space(self):
        out = sanitize_code("x = 1 ")
        assert " " not in out

    def test_empty_input(self):
        assert sanitize_code("") == ""

    def test_returns_string(self):
        assert isinstance(sanitize_code("x = 1"), str)
