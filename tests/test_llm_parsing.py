import pytest
from app.core.llm_client import _strip_markdown_fences


def test_strip_plain_json():
    raw = '[{"a": 1}]'
    assert _strip_markdown_fences(raw) == '[{"a": 1}]'


def test_strip_json_fences():
    raw = '```json\n[{"a": 1}]\n```'
    assert _strip_markdown_fences(raw) == '[{"a": 1}]'


def test_strip_plain_fences():
    raw = '```\n[{"a": 1}]\n```'
    assert _strip_markdown_fences(raw) == '[{"a": 1}]'


def test_strip_fences_with_spaces():
    raw = '  ```json  \n[{"a": 1}]\n```  '
    assert _strip_markdown_fences(raw) == '[{"a": 1}]'


def test_json_with_backtick_in_value():
    raw = '```json\n[{"a": "texto com ` backtick"}]\n```'
    result = _strip_markdown_fences(raw)
    assert result == '[{"a": "texto com ` backtick"}]'
