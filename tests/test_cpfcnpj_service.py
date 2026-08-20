import pytest
import requests
import responses

import cpfcnpj_service
from cpfcnpj_service import (
    BASE_URL,
    _formatar_cpf,
    consultar_cnpj,
    consultar_cpf,
    limpar_cpf,
    validar_dados_cliente,
)

VALID_CPF = "12345678901"
VALID_CNPJ = "29979036000140"
TOKEN = "test-token"


@pytest.fixture
def token(monkeypatch):
    monkeypatch.setattr(cpfcnpj_service, "CPFCNPJ_TOKEN", TOKEN)


@pytest.fixture
def no_token(monkeypatch):
    monkeypatch.setattr(cpfcnpj_service, "CPFCNPJ_TOKEN", "")


def api_url(doc):
    return f"{BASE_URL}/{TOKEN}/6/json/{doc}"


class TestLimparCpf:
    def test_strips_formatting(self):
        assert limpar_cpf("123.456.789-01") == VALID_CPF

    def test_already_clean_unchanged(self):
        assert limpar_cpf(VALID_CPF) == VALID_CPF

    def test_strips_spaces(self):
        assert limpar_cpf(" 123 456 789 01 ") == VALID_CPF

    def test_empty_string(self):
        assert limpar_cpf("") == ""

    def test_letters_removed(self):
        assert limpar_cpf("abc123") == "123"


class TestFormatarCpf:
    def test_formats_eleven_digits(self):
        assert _formatar_cpf(VALID_CPF) == "123.456.789-01"

    def test_reformats_already_formatted(self):
        assert _formatar_cpf("123.456.789-01") == "123.456.789-01"

    def test_returns_input_when_wrong_length(self):
        assert _formatar_cpf("123") == "123"

    def test_returns_input_when_empty(self):
        assert _formatar_cpf("") == ""


class TestConsultarCpf:
    def test_rejects_short_cpf(self, token):
        with pytest.raises(ValueError, match="CPF invalido"):
            consultar_cpf("123")

    def test_rejects_long_cpf(self, token):
        with pytest.raises(ValueError, match="CPF invalido"):
            consultar_cpf("1" * 12)

    def test_rejects_empty_cpf(self, token):
        with pytest.raises(ValueError, match="CPF invalido"):
            consultar_cpf("")

    def test_requires_token(self, no_token):
        with pytest.raises(ValueError, match="CPFCNPJ_TOKEN"):
            consultar_cpf(VALID_CPF)

    def test_validates_length_before_token(self, no_token):
        # Length check runs first, so the error is about the CPF, not the token.
        with pytest.raises(ValueError, match="CPF invalido"):
            consultar_cpf("123")

    @responses.activate
    def test_returns_normalized_payload(self, token, cpfcnpj_success):
        responses.add(responses.GET, api_url(VALID_CPF), json=cpfcnpj_success)
        out = consultar_cpf(VALID_CPF)
        assert out["cpf"] == VALID_CPF
        assert out["nome"] == "MARIA DA SILVA"
        assert out["nascimento"] == "01/01/1990"
        assert out["telefone"] == "11999998888"

    @responses.activate
    def test_strips_whitespace_from_fields(self, token, cpfcnpj_success):
        responses.add(responses.GET, api_url(VALID_CPF), json=cpfcnpj_success)
        # Fixture name is " MARIA DA SILVA " with padding.
        assert consultar_cpf(VALID_CPF)["nome"] == "MARIA DA SILVA"

    @responses.activate
    def test_cep_digits_only(self, token, cpfcnpj_success):
        responses.add(responses.GET, api_url(VALID_CPF), json=cpfcnpj_success)
        assert consultar_cpf(VALID_CPF)["endereco"]["cep"] == "01310100"

    @responses.activate
    def test_address_fields_mapped(self, token, cpfcnpj_success):
        responses.add(responses.GET, api_url(VALID_CPF), json=cpfcnpj_success)
        end = consultar_cpf(VALID_CPF)["endereco"]
        assert end["logradouro"] == "Rua das Flores"
        assert end["numero"] == "100"
        assert end["bairro"] == "Centro"
        assert end["uf"] == "SP"

    @responses.activate
    def test_accepts_formatted_cpf_input(self, token, cpfcnpj_success):
        responses.add(responses.GET, api_url(VALID_CPF), json=cpfcnpj_success)
        assert consultar_cpf("123.456.789-01")["cpf"] == VALID_CPF

    @responses.activate
    def test_missing_fields_default_to_empty(self, token):
        responses.add(responses.GET, api_url(VALID_CPF), json={})
        out = consultar_cpf(VALID_CPF)
        assert out["nome"] == ""
        assert out["endereco"]["cep"] == ""

    @responses.activate
    def test_raw_payload_preserved(self, token, cpfcnpj_success):
        responses.add(responses.GET, api_url(VALID_CPF), json=cpfcnpj_success)
        assert consultar_cpf(VALID_CPF)["raw"] == cpfcnpj_success

    @responses.activate
    def test_api_error_code_raises(self, token):
        responses.add(
            responses.GET,
            api_url(VALID_CPF),
            json={"codigo_erro": "3", "mensagem_erro": "CPF nao encontrado"},
        )
        with pytest.raises(ValueError, match="CPF nao encontrado"):
            consultar_cpf(VALID_CPF)

    @responses.activate
    def test_api_error_without_message_uses_default(self, token):
        responses.add(responses.GET, api_url(VALID_CPF), json={"codigo_erro": "9"})
        with pytest.raises(ValueError, match="desconhecido"):
            consultar_cpf(VALID_CPF)

    @responses.activate
    def test_http_error_propagates(self, token):
        responses.add(responses.GET, api_url(VALID_CPF), status=500)
        with pytest.raises(requests.HTTPError):
            consultar_cpf(VALID_CPF)


class TestConsultarCnpj:
    def test_rejects_wrong_length(self, token):
        with pytest.raises(ValueError, match="CNPJ invalido"):
            consultar_cnpj("123")

    def test_requires_token(self, no_token):
        with pytest.raises(ValueError, match="CPFCNPJ_TOKEN"):
            consultar_cnpj(VALID_CNPJ)

    @responses.activate
    def test_returns_raw_payload(self, token):
        payload = {"nome": "INSS", "cnpj": VALID_CNPJ}
        responses.add(responses.GET, api_url(VALID_CNPJ), json=payload)
        assert consultar_cnpj(VALID_CNPJ) == payload

    @responses.activate
    def test_accepts_formatted_cnpj(self, token):
        responses.add(responses.GET, api_url(VALID_CNPJ), json={"nome": "INSS"})
        assert consultar_cnpj("29.979.036/0001-40")["nome"] == "INSS"

    @responses.activate
    def test_api_error_raises(self, token):
        responses.add(
            responses.GET,
            api_url(VALID_CNPJ),
            json={"codigo_erro": "3", "mensagem_erro": "nao encontrado"},
        )
        with pytest.raises(ValueError, match="nao encontrado"):
            consultar_cnpj(VALID_CNPJ)


class TestValidarDadosCliente:
    @responses.activate
    def test_exact_name_match_has_no_alerts(self, token, cpfcnpj_success):
        responses.add(responses.GET, api_url(VALID_CPF), json=cpfcnpj_success)
        out = validar_dados_cliente(VALID_CPF, "MARIA DA SILVA")
        assert out["validacao"]["alertas"] == []

    @responses.activate
    def test_name_match_is_case_insensitive(self, token, cpfcnpj_success):
        responses.add(responses.GET, api_url(VALID_CPF), json=cpfcnpj_success)
        out = validar_dados_cliente(VALID_CPF, "maria da silva")
        assert out["validacao"]["alertas"] == []

    @responses.activate
    def test_expected_name_substring_accepted(self, token, cpfcnpj_success):
        responses.add(responses.GET, api_url(VALID_CPF), json=cpfcnpj_success)
        out = validar_dados_cliente(VALID_CPF, "MARIA")
        assert out["validacao"]["alertas"] == []

    @responses.activate
    def test_divergent_name_raises_alert(self, token, cpfcnpj_success):
        responses.add(responses.GET, api_url(VALID_CPF), json=cpfcnpj_success)
        out = validar_dados_cliente(VALID_CPF, "JOAO PEREIRA")
        assert any("Nome diverge" in a for a in out["validacao"]["alertas"])

    @responses.activate
    def test_no_expected_name_skips_name_check(self, token, cpfcnpj_success):
        responses.add(responses.GET, api_url(VALID_CPF), json=cpfcnpj_success)
        out = validar_dados_cliente(VALID_CPF)
        assert out["validacao"]["alertas"] == []

    @responses.activate
    def test_missing_cep_raises_alert(self, token, cpfcnpj_success):
        payload = {**cpfcnpj_success, "cep": ""}
        responses.add(responses.GET, api_url(VALID_CPF), json=payload)
        out = validar_dados_cliente(VALID_CPF, "MARIA DA SILVA")
        assert any("CEP" in a for a in out["validacao"]["alertas"])

    @responses.activate
    def test_both_alerts_can_stack(self, token, cpfcnpj_success):
        payload = {**cpfcnpj_success, "cep": ""}
        responses.add(responses.GET, api_url(VALID_CPF), json=payload)
        out = validar_dados_cliente(VALID_CPF, "JOAO PEREIRA")
        assert len(out["validacao"]["alertas"]) == 2

    @responses.activate
    def test_preserves_underlying_data(self, token, cpfcnpj_success):
        responses.add(responses.GET, api_url(VALID_CPF), json=cpfcnpj_success)
        out = validar_dados_cliente(VALID_CPF, "MARIA DA SILVA")
        assert out["nome"] == "MARIA DA SILVA"
        assert out["endereco"]["uf"] == "SP"

    @responses.activate
    def test_cpf_valido_flag_always_true(self, token, cpfcnpj_success):
        # The flag is hardcoded; it does not reflect check-digit validation.
        responses.add(responses.GET, api_url(VALID_CPF), json=cpfcnpj_success)
        out = validar_dados_cliente(VALID_CPF, "JOAO PEREIRA")
        assert out["validacao"]["cpf_valido"] is True

    def test_invalid_cpf_propagates(self, token):
        with pytest.raises(ValueError, match="CPF invalido"):
            validar_dados_cliente("123")


class TestGerarDeclaracaoResidencia:
    """gerar_declaracao_residencia imports python-docx lazily, and python-docx is
    absent from requirements.txt. processar_lote_v2 calls it whenever an address
    came from the CPFCNPJ lookup, so this guards the dependency."""

    def test_python_docx_is_installed(self):
        pytest.importorskip(
            "docx",
            reason="python-docx missing from requirements.txt; "
            "gerar_declaracao_residencia raises ModuleNotFoundError when called",
        )

    def test_writes_docx_to_output_dir(self, tmp_path):
        pytest.importorskip("docx")
        from cpfcnpj_service import gerar_declaracao_residencia

        path = gerar_declaracao_residencia(
            nome="Maria da Silva", cpf=VALID_CPF, logradouro="Rua das Flores",
            numero="100", bairro="Centro", cidade="São Paulo", uf="SP",
            cep="01310100", output_dir=str(tmp_path),
        )
        assert path.endswith(f"declaracao_residencia_{VALID_CPF}.docx")
        assert (tmp_path / f"declaracao_residencia_{VALID_CPF}.docx").exists()
