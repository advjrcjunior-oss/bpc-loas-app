import pytest

import processar_lote_v2
from processar_lote_v2 import extrair_dados_partes


@pytest.fixture
def no_network(monkeypatch):
    """Neutralize ViaCEP and CPFCNPJ lookups so address logic is tested in isolation."""
    monkeypatch.setattr(processar_lote_v2, "validar_cep", lambda cep: None)
    monkeypatch.setattr(processar_lote_v2, "buscar_cep_por_endereco", lambda *a: None)
    monkeypatch.setattr(processar_lote_v2, "completar_endereco_por_cep", lambda cep: None)
    monkeypatch.setattr(
        processar_lote_v2,
        "consultar_cpf",
        lambda cpf: (_ for _ in ()).throw(AssertionError("should not be called")),
    )


class TestExtrairDadosPartes:
    def test_representative_plus_minor(self, no_network):
        out = extrair_dados_partes({
            "representante_nome": "MARIA SILVA",
            "representante_cpf": "111.222.333-44",
            "nome": "JOAO SILVA",
            "cpf": "555.666.777-88",
        })
        assert out["representante"]["nome"] == "MARIA SILVA"
        assert out["menor"]["nome"] == "JOAO SILVA"
        assert "autora" not in out

    def test_adult_alone_becomes_autora(self, no_network):
        out = extrair_dados_partes({"nome": "DENISE SOUZA", "cpf": "555.666.777-88"})
        assert out["autora"]["nome"] == "DENISE SOUZA"
        assert "representante" not in out
        assert "menor" not in out

    def test_no_names_yields_no_party(self, no_network):
        out = extrair_dados_partes({})
        assert "autora" not in out
        assert "representante" not in out

    def test_alternate_field_names_accepted(self, no_network):
        out = extrair_dados_partes({
            "nome_representante": "MARIA",
            "cpf_representante": "111",
            "nome_autor": "JOAO",
            "cpf_autor": "222",
        })
        assert out["representante"]["nome"] == "MARIA"
        assert out["menor"]["nome"] == "JOAO"

    def test_nested_representative_dict_accepted(self, no_network):
        out = extrair_dados_partes({
            "representante": {"nome": "MARIA"},
            "nome": "JOAO",
        })
        assert out["representante"]["nome"] == "MARIA"

    def test_minor_profession_is_desempregado(self, no_network):
        out = extrair_dados_partes({"representante_nome": "M", "nome": "J"})
        assert out["menor"]["profissao"] == "DESEMPREGADO"

    def test_mother_gets_do_lar_default(self, no_network):
        out = extrair_dados_partes({
            "representante_nome": "MARIA",
            "nome": "JOAO",
            "representante_parentesco": "mae",
        })
        assert "DO LAR" in out["representante"]["profissao"]

    @pytest.mark.parametrize("parentesco", ["pai", "tutor", "curador"])
    def test_father_tutor_curator_get_desempregado(self, parentesco, no_network):
        out = extrair_dados_partes({
            "representante_nome": "JOSE",
            "nome": "JOAO",
            "representante_parentesco": parentesco,
        })
        assert out["representante"]["profissao"] == "Desempregado"

    def test_parentesco_match_is_case_insensitive(self, no_network):
        out = extrair_dados_partes({
            "representante_nome": "JOSE",
            "nome": "JOAO",
            "representante_parentesco": "PAI",
        })
        assert out["representante"]["profissao"] == "Desempregado"

    def test_explicit_profession_overrides_default(self, no_network):
        out = extrair_dados_partes({
            "representante_nome": "MARIA",
            "nome": "JOAO",
            "profissao_representante": "PROFESSORA",
        })
        assert out["representante"]["profissao"] == "PROFESSORA"

    def test_missing_numero_defaults_to_sn(self, no_network):
        out = extrair_dados_partes({"nome": "DENISE", "logradouro": "Rua X"})
        assert out["autora"]["endereco_numero"] == "S/N"

    def test_address_fields_copied_to_both_parties(self, no_network):
        out = extrair_dados_partes({
            "representante_nome": "MARIA",
            "nome": "JOAO",
            "logradouro": "Rua X",
            "cidade": "Sao Paulo",
            "estado": "SP",
        })
        assert out["representante"]["endereco_logradouro"] == "Rua X"
        assert out["menor"]["endereco_logradouro"] == "Rua X"

    def test_uf_falls_back_to_uf_key(self, no_network):
        out = extrair_dados_partes({"nome": "D", "uf": "MG"})
        assert out["uf"] == "MG"

    def test_cidade_falls_back_to_municipio(self, no_network):
        out = extrair_dados_partes({"nome": "D", "municipio": "Belo Horizonte"})
        assert out["cidade"] == "Belo Horizonte"

    def test_rua_key_used_when_logradouro_absent(self, no_network):
        out = extrair_dados_partes({"nome": "D", "rua": "Avenida Brasil"})
        assert out["autora"]["endereco_logradouro"] == "Avenida Brasil"

    def test_endereco_string_parsed_into_parts(self, no_network):
        out = extrair_dados_partes({
            "nome": "DENISE",
            "endereco": "Rua das Flores, 123, Centro",
        })
        end = out["autora"]
        assert end["endereco_logradouro"] == "Rua das Flores"
        assert end["endereco_numero"] == "123"
        assert end["endereco_bairro"] == "Centro"

    def test_endereco_string_with_non_numeric_second_part_is_bairro(self, no_network):
        out = extrair_dados_partes({"nome": "D", "endereco": "Rua X, Centro"})
        assert out["autora"]["endereco_bairro"] == "Centro"
        assert out["autora"]["endereco_numero"] == "S/N"

    def test_endereco_string_accepts_sn_marker(self, no_network):
        out = extrair_dados_partes({"nome": "D", "endereco": "Rua X, S/N, Centro"})
        assert out["autora"]["endereco_numero"] == "S/N"

    def test_explicit_logradouro_wins_over_endereco_string(self, no_network):
        out = extrair_dados_partes({
            "nome": "D",
            "logradouro": "Rua Direta",
            "endereco": "Rua Ignorada, 1, Centro",
        })
        assert out["autora"]["endereco_logradouro"] == "Rua Direta"

    def test_valid_cep_fills_missing_city_and_uf(self, monkeypatch):
        monkeypatch.setattr(
            processar_lote_v2,
            "validar_cep",
            lambda cep: {
                "localidade": "São Paulo",
                "uf": "SP",
                "bairro": "Bela Vista",
                "logradouro": "Avenida Paulista",
            },
        )
        out = extrair_dados_partes({"nome": "DENISE", "cep": "01310-100"})
        assert out["cidade"] == "São Paulo"
        assert out["uf"] == "SP"
        assert out["autora"]["endereco_cep"] == "01310100"

    def test_extracted_values_win_over_viacep(self, monkeypatch):
        monkeypatch.setattr(
            processar_lote_v2,
            "validar_cep",
            lambda cep: {"localidade": "São Paulo", "uf": "SP"},
        )
        out = extrair_dados_partes({
            "nome": "D", "cep": "01310100", "cidade": "Santos", "estado": "SP",
        })
        assert out["cidade"] == "Santos"

    def test_invalid_cep_triggers_address_search(self, monkeypatch):
        monkeypatch.setattr(processar_lote_v2, "validar_cep", lambda cep: None)
        monkeypatch.setattr(
            processar_lote_v2, "buscar_cep_por_endereco", lambda *a: "04567000"
        )
        out = extrair_dados_partes({
            "nome": "D", "cep": "00000000",
            "cidade": "Santos", "estado": "SP", "logradouro": "Rua X",
        })
        assert out["autora"]["endereco_cep"] == "04567000"

    def test_cpfcnpj_fallback_when_no_cep(self, monkeypatch):
        monkeypatch.setattr(processar_lote_v2, "validar_cep", lambda cep: None)
        monkeypatch.setattr(processar_lote_v2, "buscar_cep_por_endereco", lambda *a: None)
        monkeypatch.setattr(processar_lote_v2, "completar_endereco_por_cep", lambda c: None)
        monkeypatch.setattr(
            processar_lote_v2,
            "consultar_cpf",
            lambda cpf: {"endereco": {
                "cep": "01310100", "logradouro": "Avenida Paulista",
                "numero": "1000", "bairro": "Bela Vista",
                "cidade": "São Paulo", "uf": "SP",
            }},
        )
        out = extrair_dados_partes({"nome": "DENISE", "cpf": "12345678901"})
        assert out["endereco_via_cpfcnpj"] is True
        assert out["autora"]["endereco_cep"] == "01310100"
        assert out["autora"]["endereco_logradouro"] == "Avenida Paulista"

    def test_cpfcnpj_fallback_skipped_for_malformed_cpf(self, no_network):
        # no_network makes consultar_cpf raise if called.
        out = extrair_dados_partes({"nome": "DENISE", "cpf": "123"})
        assert out["endereco_via_cpfcnpj"] is False

    def test_cpfcnpj_failure_is_swallowed(self, monkeypatch):
        monkeypatch.setattr(processar_lote_v2, "validar_cep", lambda cep: None)
        monkeypatch.setattr(
            processar_lote_v2,
            "consultar_cpf",
            lambda cpf: (_ for _ in ()).throw(RuntimeError("API down")),
        )
        out = extrair_dados_partes({"nome": "DENISE", "cpf": "12345678901"})
        assert out["endereco_via_cpfcnpj"] is False
        assert out["autora"]["nome"] == "DENISE"

    def test_flag_false_when_address_came_from_documents(self, no_network):
        out = extrair_dados_partes({"nome": "D", "logradouro": "Rua X"})
        assert out["endereco_via_cpfcnpj"] is False

    def test_representative_cpf_preferred_for_lookup(self, monkeypatch):
        seen = []
        monkeypatch.setattr(processar_lote_v2, "validar_cep", lambda cep: None)
        monkeypatch.setattr(
            processar_lote_v2,
            "consultar_cpf",
            lambda cpf: seen.append(cpf) or {"endereco": {}},
        )
        extrair_dados_partes({
            "representante_nome": "MARIA", "representante_cpf": "11111111111",
            "nome": "JOAO", "cpf": "22222222222",
        })
        assert seen == ["11111111111"]

    def test_numeric_cep_input_is_stringified(self, monkeypatch):
        monkeypatch.setattr(processar_lote_v2, "validar_cep", lambda cep: None)
        monkeypatch.setattr(processar_lote_v2, "buscar_cep_por_endereco", lambda *a: None)
        out = extrair_dados_partes({"nome": "D", "cep": 1310100})
        assert isinstance(out["autora"]["endereco_cep"], str)

    def test_always_returns_cidade_and_uf_keys(self, no_network):
        out = extrair_dados_partes({})
        assert "cidade" in out
        assert "uf" in out
