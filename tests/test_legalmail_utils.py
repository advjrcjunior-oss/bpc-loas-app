import pytest
import responses

import legalmail_service
from legalmail_service import (
    ORDEM_DOCUMENTOS,
    UF_TRIBUNAL_MAP,
    VIACEP_BASE,
    buscar_cep_por_endereco,
    completar_endereco_por_cep,
    ordenar_documentos,
    validar_cep,
    validar_pdf,
    validar_procuracao,
)

CEP = "01310100"


def cep_url(cep=CEP):
    return f"{VIACEP_BASE}/{cep}/json/"


class TestValidarCep:
    @responses.activate
    def test_valid_cep_returns_address(self, viacep_success):
        responses.add(responses.GET, cep_url(), json=viacep_success)
        assert validar_cep(CEP)["uf"] == "SP"

    @responses.activate
    def test_accepts_formatted_cep(self, viacep_success):
        responses.add(responses.GET, cep_url(), json=viacep_success)
        assert validar_cep("01310-100") is not None

    @responses.activate
    def test_erro_flag_returns_none(self):
        responses.add(responses.GET, cep_url(), json={"erro": True})
        assert validar_cep(CEP) is None

    @responses.activate
    def test_http_error_returns_none(self):
        responses.add(responses.GET, cep_url(), status=500)
        assert validar_cep(CEP) is None

    @responses.activate
    def test_network_failure_returns_none(self):
        responses.add(responses.GET, cep_url(), body=ConnectionError("boom"))
        assert validar_cep(CEP) is None

    def test_short_cep_skips_request(self):
        # No responses registered: a request would raise, so this proves it short-circuits.
        assert validar_cep("123") is None

    def test_long_cep_skips_request(self):
        assert validar_cep("1" * 9) is None

    def test_empty_cep_skips_request(self):
        assert validar_cep("") is None

    def test_none_cep_skips_request(self):
        assert validar_cep(None) is None


class TestBuscarCepPorEndereco:
    @responses.activate
    def test_returns_first_match_without_dash(self):
        url = f"{VIACEP_BASE}/SP/São Paulo/Avenida+Paulista/json/"
        responses.add(responses.GET, url, json=[{"cep": "01310-100"}])
        assert buscar_cep_por_endereco("SP", "São Paulo", "Avenida Paulista") == "01310100"

    @responses.activate
    def test_empty_result_returns_none(self):
        responses.add(
            responses.GET,
            f"{VIACEP_BASE}/SP/São Paulo/Rua+X/json/",
            json=[],
        )
        assert buscar_cep_por_endereco("SP", "São Paulo", "Rua X") is None

    @responses.activate
    def test_non_list_response_returns_none(self):
        responses.add(
            responses.GET,
            f"{VIACEP_BASE}/SP/São Paulo/Rua+X/json/",
            json={"erro": True},
        )
        assert buscar_cep_por_endereco("SP", "São Paulo", "Rua X") is None

    @responses.activate
    def test_http_error_returns_none(self):
        responses.add(
            responses.GET,
            f"{VIACEP_BASE}/SP/São Paulo/Rua+X/json/",
            status=404,
        )
        assert buscar_cep_por_endereco("SP", "São Paulo", "Rua X") is None

    @responses.activate
    def test_long_street_is_truncated_to_40_chars(self):
        rua = "A" * 60
        responses.add(
            responses.GET,
            f"{VIACEP_BASE}/SP/Cidade/{'A' * 40}/json/",
            json=[{"cep": "01310-100"}],
        )
        assert buscar_cep_por_endereco("SP", "Cidade", rua) == "01310100"

    @responses.activate
    def test_network_failure_returns_none(self):
        responses.add(
            responses.GET,
            f"{VIACEP_BASE}/SP/Cidade/Rua/json/",
            body=ConnectionError("boom"),
        )
        assert buscar_cep_por_endereco("SP", "Cidade", "Rua") is None


class TestCompletarEnderecoPorCep:
    @responses.activate
    def test_maps_viacep_fields_to_endereco_keys(self, viacep_success):
        responses.add(responses.GET, cep_url(), json=viacep_success)
        out = completar_endereco_por_cep(CEP)
        assert out == {
            "endereco_cep": "01310100",
            "endereco_logradouro": "Avenida Paulista",
            "endereco_bairro": "Bela Vista",
            "endereco_cidade": "São Paulo",
            "endereco_uf": "SP",
        }

    @responses.activate
    def test_invalid_cep_returns_none(self):
        responses.add(responses.GET, cep_url(), json={"erro": True})
        assert completar_endereco_por_cep(CEP) is None

    @responses.activate
    def test_missing_fields_become_empty_strings(self):
        responses.add(responses.GET, cep_url(), json={"cep": "01310-100"})
        out = completar_endereco_por_cep(CEP)
        assert out["endereco_logradouro"] == ""
        assert out["endereco_uf"] == ""

    def test_malformed_cep_returns_none(self):
        assert completar_endereco_por_cep("123") is None


class TestValidarPdf:
    def test_missing_file_rejected(self, tmp_path):
        ok, msg = validar_pdf(str(tmp_path / "nope.pdf"))
        assert ok is False
        assert "nao existe" in msg

    def test_tiny_file_rejected(self, tmp_path):
        p = tmp_path / "tiny.pdf"
        p.write_bytes(b"%PDF-")
        ok, msg = validar_pdf(str(p))
        assert ok is False
        assert "muito pequeno" in msg

    def test_empty_file_rejected(self, tmp_path):
        p = tmp_path / "empty.pdf"
        p.write_bytes(b"")
        assert validar_pdf(str(p))[0] is False

    def test_valid_pdf_accepted(self, pdf_file):
        assert validar_pdf(pdf_file)[0] is True

    @pytest.mark.skipif(
        legalmail_service.PyPDF2 is None, reason="requires PyPDF2"
    )
    def test_non_pdf_header_rejected(self, tmp_path):
        p = tmp_path / "fake.pdf"
        p.write_bytes(b"PK\x03\x04" + b"x" * 200)  # DOCX magic bytes
        ok, msg = validar_pdf(str(p))
        assert ok is False
        assert "header incorreto" in msg

    @pytest.mark.skipif(
        legalmail_service.PyPDF2 is not None, reason="only when PyPDF2 missing"
    )
    def test_falls_back_to_permissive_without_pypdf2(self, tmp_path):
        p = tmp_path / "fake.pdf"
        p.write_bytes(b"PK\x03\x04" + b"x" * 200)
        ok, msg = validar_pdf(str(p))
        assert ok is True
        assert "PyPDF2 nao disponivel" in msg

    def test_size_check_runs_before_pypdf2_fallback(self, tmp_path):
        # A 50-byte non-PDF is rejected on size regardless of PyPDF2 availability.
        p = tmp_path / "small.pdf"
        p.write_bytes(b"x" * 50)
        assert validar_pdf(str(p))[0] is False


class TestValidarProcuracao:
    def test_missing_file_rejected(self, tmp_path):
        ok, msg = validar_procuracao(str(tmp_path / "nope.pdf"))
        assert ok is False
        assert "nao existe" in msg

    def test_returns_two_tuple(self, pdf_file):
        result = validar_procuracao(pdf_file)
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], bool)

    @pytest.mark.skipif(
        legalmail_service.PyPDF2 is not None, reason="only when PyPDF2 missing"
    )
    def test_falls_back_to_permissive_without_pypdf2(self, pdf_file):
        ok, msg = validar_procuracao(pdf_file)
        assert ok is True
        assert "PyPDF2 nao disponivel" in msg


class TestOrdenarDocumentos:
    def test_orders_by_ordem_documentos_not_numerically(self):
        # ORDEM_DOCUMENTOS puts "4" before "3".
        assert ordenar_documentos(["3-contrato.pdf", "4-gratuidade.pdf"]) == [
            "4-gratuidade.pdf",
            "3-contrato.pdf",
        ]

    def test_petition_sorts_first(self):
        out = ordenar_documentos(["7-residencia.pdf", "1-peticao.pdf", "2-procuracao.pdf"])
        assert out[0] == "1-peticao.pdf"

    def test_unknown_prefix_sorts_after_known(self):
        out = ordenar_documentos(["99-outro.pdf", "1-peticao.pdf"])
        assert out == ["1-peticao.pdf", "99-outro.pdf"]

    def test_no_prefix_gets_default_priority(self):
        out = ordenar_documentos(["semprefixo.pdf", "1-peticao.pdf"])
        assert out[0] == "1-peticao.pdf"

    def test_quesitos_medicos_before_sociais(self):
        out = ordenar_documentos(["QUESITOS_SOCIAIS.pdf", "QUESITOS_MEDICOS.pdf"])
        assert out == ["QUESITOS_MEDICOS.pdf", "QUESITOS_SOCIAIS.pdf"]

    def test_generic_quesitos_recognized(self):
        out = ordenar_documentos(["QUESITOS.pdf", "1-peticao.pdf"])
        assert out[0] == "1-peticao.pdf"

    def test_planilha_sorts_after_named_quesitos(self):
        out = ordenar_documentos(["PLANILHA_CALCULO.pdf", "QUESITOS_MEDICOS.pdf"])
        assert out[-1] == "PLANILHA_CALCULO.pdf"

    def test_planilha_sorts_after_numbered_docs(self):
        out = ordenar_documentos(["PLANILHA_CALCULO.pdf", "1-peticao.pdf", "23-outros.pdf"])
        assert out[-1] == "PLANILHA_CALCULO.pdf"

    def test_generic_quesitos_outranks_planilha(self):
        # PLANILHA resolves to its ORDEM_DOCUMENTOS index (25) while a generically
        # named quesitos file gets the hardcoded 98, so it lands after the planilha
        # that ORDEM_DOCUMENTOS documents as "ultimo". Current behavior, likely a bug.
        out = ordenar_documentos(["PLANILHA_CALCULO.pdf", "QUESITOS.pdf"])
        assert out[-1] == "QUESITOS.pdf"

    def test_matching_is_case_insensitive(self):
        out = ordenar_documentos(["planilha_calc.pdf", "1-x.pdf"])
        assert out[-1] == "planilha_calc.pdf"

    def test_uses_basename_for_prefix_detection(self):
        out = ordenar_documentos(["/tmp/9/2-procuracao.pdf", "/tmp/9/1-peticao.pdf"])
        assert out[0].endswith("1-peticao.pdf")

    def test_empty_list(self):
        assert ordenar_documentos([]) == []

    def test_is_stable_and_preserves_all_items(self):
        docs = ["5-rg.pdf", "1-peticao.pdf", "2-proc.pdf", "PLANILHA.pdf"]
        assert sorted(ordenar_documentos(docs)) == sorted(docs)

    def test_full_realistic_ordering(self):
        docs = ["7-residencia.pdf", "2-procuracao.pdf", "1-peticao.pdf", "5-rg.pdf"]
        assert ordenar_documentos(docs) == [
            "1-peticao.pdf",
            "2-procuracao.pdf",
            "5-rg.pdf",
            "7-residencia.pdf",
        ]


class TestUfTribunalMap:
    def test_covers_all_26_states_plus_df(self):
        assert len(UF_TRIBUNAL_MAP) == 27

    @pytest.mark.parametrize(
        "uf,sistema",
        [("SP", "pje"), ("MG", "pje"), ("DF", "pje"), ("BA", "pje")],
    )
    def test_pje_states(self, uf, sistema):
        assert UF_TRIBUNAL_MAP[uf]["sistema"] == sistema

    @pytest.mark.parametrize("uf", ["RJ", "ES", "PR", "SC", "RS"])
    def test_eproc_states(self, uf):
        assert "eproc" in UF_TRIBUNAL_MAP[uf]["sistema"]

    @pytest.mark.parametrize(
        "uf,trf",
        [("DF", "TRF-1"), ("RJ", "TRF-2"), ("SP", "TRF-3"), ("PR", "TRF-4"),
         ("PE", "TRF-5"), ("MG", "TRF-6")],
    )
    def test_trf_assignment(self, uf, trf):
        assert UF_TRIBUNAL_MAP[uf]["trf"] == trf

    def test_every_entry_has_required_keys(self):
        for uf, info in UF_TRIBUNAL_MAP.items():
            assert {"trf", "sistema", "uf_tribunal"} <= info.keys(), uf

    def test_uf_tribunal_matches_key(self):
        for uf, info in UF_TRIBUNAL_MAP.items():
            assert info["uf_tribunal"] == uf


class TestOrdemDocumentos:
    def test_has_no_duplicates(self):
        assert len(ORDEM_DOCUMENTOS) == len(set(ORDEM_DOCUMENTOS))

    def test_petition_is_first(self):
        assert ORDEM_DOCUMENTOS[0] == "1"

    def test_gratuidade_precedes_honorarios(self):
        assert ORDEM_DOCUMENTOS.index("4") < ORDEM_DOCUMENTOS.index("3")
