import pytest

import app
from app import (
    _detect_genero,
    _match_comarca,
    _normalize_text,
    _parse_brl,
    _rate_limit_check,
    extract_json,
)


@pytest.fixture(autouse=True)
def clear_rate_limits():
    app._rate_limit_store.clear()
    yield
    app._rate_limit_store.clear()


class TestExtractJson:
    def test_plain_object(self):
        assert extract_json('{"a": 1}') == {"a": 1}

    def test_tolerates_surrounding_whitespace(self):
        assert extract_json('  \n {"a": 1} \n ') == {"a": 1}

    def test_markdown_fenced_json(self):
        assert extract_json('```json\n{"a": 1}\n```') == {"a": 1}

    def test_markdown_fence_without_language(self):
        assert extract_json('```\n{"a": 1}\n```') == {"a": 1}

    def test_json_with_leading_prose(self):
        assert extract_json('Segue o resultado:\n{"a": 1}') == {"a": 1}

    def test_json_with_trailing_prose(self):
        assert extract_json('{"a": 1}\nEspero ter ajudado.') == {"a": 1}

    def test_nested_structures_preserved(self):
        out = extract_json('{"a": {"b": [1, 2]}}')
        assert out["a"]["b"] == [1, 2]

    def test_non_ascii_preserved(self):
        assert extract_json('{"nome": "João"}')["nome"] == "João"

    def test_no_json_returns_none(self):
        assert extract_json("nenhum json aqui") is None

    def test_malformed_json_returns_none(self):
        assert extract_json('{"a": }') is None

    def test_empty_string_returns_none(self):
        assert extract_json("") is None

    def test_empty_object(self):
        assert extract_json("{}") == {}


class TestParseBrl:
    def test_br_format_with_thousands(self):
        assert _parse_brl("R$ 29.231,70") == 29231.70

    def test_br_decimal_only(self):
        assert _parse_brl("29231,70") == 29231.70

    def test_us_format_with_thousands(self):
        assert _parse_brl("29,231.70") == 29231.70

    def test_us_decimal_only(self):
        assert _parse_brl("29231.70") == 29231.70

    def test_plain_integer_string(self):
        assert _parse_brl("1320") == 1320.0

    def test_strips_currency_symbol_and_spaces(self):
        assert _parse_brl("  R$1.320,00  ") == 1320.0

    def test_accepts_int(self):
        assert _parse_brl(1320) == 1320.0

    def test_accepts_float_and_rounds(self):
        assert _parse_brl(1320.567) == 1320.57

    def test_comma_with_three_digits_treated_as_thousands(self):
        assert _parse_brl("29,231") == 29231.0

    def test_none_returns_none(self):
        assert _parse_brl(None) is None

    def test_empty_string_returns_none(self):
        assert _parse_brl("") is None

    def test_currency_symbol_only_returns_none(self):
        assert _parse_brl("R$") is None

    def test_non_numeric_returns_none(self):
        assert _parse_brl("abc") is None

    def test_zero(self):
        assert _parse_brl("0,00") == 0.0

    def test_rounds_to_two_places(self):
        assert _parse_brl("1,999") == 1999.0


class TestNormalizeText:
    def test_strips_accents(self):
        assert _normalize_text("São Paulo") == "sao paulo"

    def test_lowercases(self):
        assert _normalize_text("BRASILIA") == "brasilia"

    def test_trims_whitespace(self):
        assert _normalize_text("  Santos  ") == "santos"

    def test_handles_cedilla(self):
        assert _normalize_text("Conceição") == "conceicao"

    def test_coerces_non_string(self):
        assert _normalize_text(123) == "123"

    def test_empty_string(self):
        assert _normalize_text("") == ""


class TestMatchComarca:
    def test_exact_match(self):
        assert _match_comarca("Santos", ["Santos", "Sao Paulo"]) == "Santos"

    def test_match_ignores_accents(self):
        assert _match_comarca("Sao Paulo", ["São Paulo"]) == "São Paulo"

    def test_match_ignores_case(self):
        assert _match_comarca("SANTOS", ["Santos"]) == "Santos"

    def test_substring_match(self):
        assert _match_comarca("Santos", ["Comarca de Santos"]) == "Comarca de Santos"

    def test_reverse_substring_match(self):
        assert _match_comarca("Comarca de Santos", ["Santos"]) == "Santos"

    def test_word_overlap_fallback(self):
        out = _match_comarca("Ribeirao Preto SP", ["Franca", "Ribeirao Bonito"])
        assert out == "Ribeirao Bonito"

    def test_no_match_returns_none(self):
        assert _match_comarca("Manaus", ["Santos", "Franca"]) is None

    def test_empty_options_returns_none(self):
        assert _match_comarca("Santos", []) is None

    def test_exact_match_preferred_over_substring(self):
        assert _match_comarca("Santos", ["Comarca de Santos", "Santos"]) == "Santos"


class TestDetectGenero:
    @pytest.mark.parametrize("nome", ["MARIA SILVA", "Ana Souza", "Julia Lima"])
    def test_known_feminine_names(self, nome):
        assert _detect_genero(nome) == "FEMININO"

    @pytest.mark.parametrize("nome", ["Fernanda", "Gabriela", "Patricia", "Vitoria"])
    def test_more_feminine_names(self, nome):
        assert _detect_genero(nome) == "FEMININO"

    def test_feminine_ending_a(self):
        assert _detect_genero("Rosangela Pereira") == "FEMININO"

    @pytest.mark.parametrize("nome", ["Joao Silva", "Carlos", "Pedro Souza"])
    def test_masculine_names(self, nome):
        assert _detect_genero(nome) == "MASCULINO"

    @pytest.mark.parametrize("nome", ["Luca", "Josua", "Noa"])
    def test_masculine_exceptions_ending_in_a(self, nome):
        assert _detect_genero(nome) == "MASCULINO"

    def test_uses_first_name_only(self):
        assert _detect_genero("Maria dos Santos Neto") == "FEMININO"

    def test_case_insensitive(self):
        assert _detect_genero("maria silva") == "FEMININO"

    def test_leading_whitespace_tolerated(self):
        assert _detect_genero("   Maria") == "FEMININO"

    def test_empty_defaults_to_masculine(self):
        assert _detect_genero("") == "MASCULINO"

    def test_none_defaults_to_masculine(self):
        assert _detect_genero(None) == "MASCULINO"


class TestRateLimitCheck:
    def test_allows_first_request(self):
        assert _rate_limit_check("k", max_requests=3) is True

    def test_allows_up_to_limit(self):
        assert all(_rate_limit_check("k", max_requests=3) for _ in range(3))

    def test_blocks_beyond_limit(self):
        for _ in range(3):
            _rate_limit_check("k", max_requests=3)
        assert _rate_limit_check("k", max_requests=3) is False

    def test_keys_are_isolated(self):
        for _ in range(3):
            _rate_limit_check("a", max_requests=3)
        assert _rate_limit_check("b", max_requests=3) is True

    def test_window_expiry_allows_again(self, monkeypatch):
        for _ in range(3):
            _rate_limit_check("k", max_requests=3, window_seconds=60)
        real_time = app._time_module.time

        monkeypatch.setattr(app._time_module, "time", lambda: real_time() + 61)
        assert _rate_limit_check("k", max_requests=3, window_seconds=60) is True

    def test_limit_of_zero_blocks_everything(self):
        assert _rate_limit_check("k", max_requests=0) is False
