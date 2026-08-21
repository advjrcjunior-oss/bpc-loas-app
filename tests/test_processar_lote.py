import json

import pytest

import processar_lote_v2
from processar_lote_v2 import (
    classify_tribunal,
    load_checkpoint,
    save_checkpoint,
    validate_pasta_pre,
)


@pytest.fixture
def checkpoint_file(tmp_path, monkeypatch):
    path = str(tmp_path / "checkpoint.json")
    monkeypatch.setattr(processar_lote_v2, "CHECKPOINT_FILE", path)
    return path


def make_pasta(tmp_path, filenames, name="CLIENTE"):
    pasta = tmp_path / name
    pasta.mkdir()
    for fn in filenames:
        (pasta / fn).write_bytes(b"%PDF-" + b"x" * 200)
    return str(pasta)


class TestCheckpoint:
    def test_round_trip(self, checkpoint_file):
        save_checkpoint(["a"], ["b", "c"], ["err"], "a")
        cp = load_checkpoint()
        assert cp["completed"] == ["a"]
        assert cp["pending"] == ["b", "c"]
        assert cp["errors"] == ["err"]
        assert cp["current"] == "a"

    def test_records_timestamp(self, checkpoint_file):
        save_checkpoint([], [], [], "x")
        assert "timestamp" in load_checkpoint()

    def test_missing_file_returns_none(self, checkpoint_file):
        assert load_checkpoint() is None

    def test_overwrites_previous(self, checkpoint_file):
        save_checkpoint(["old"], [], [], "old")
        save_checkpoint(["new"], [], [], "new")
        assert load_checkpoint()["completed"] == ["new"]

    def test_written_as_readable_json(self, checkpoint_file):
        save_checkpoint(["a"], [], [], "a")
        with open(checkpoint_file, encoding="utf-8") as f:
            assert json.load(f)["completed"] == ["a"]

    def test_preserves_non_ascii_paths(self, checkpoint_file):
        save_checkpoint(["JOÃO"], ["MARIA/AÇÃO"], [], "JOÃO")
        cp = load_checkpoint()
        assert cp["completed"] == ["JOÃO"]
        assert cp["pending"] == ["MARIA/AÇÃO"]

    def test_empty_lists(self, checkpoint_file):
        save_checkpoint([], [], [], "")
        assert load_checkpoint()["completed"] == []


class TestClassifyTribunal:
    @pytest.mark.parametrize("uf", ["SP", "MG", "DF", "BA", "PE"])
    def test_pje_states(self, uf, monkeypatch):
        monkeypatch.setattr(processar_lote_v2, "detect_uf_from_folder", lambda p: uf)
        tipo, info = classify_tribunal("/qualquer/pasta")
        assert tipo == "pje"
        assert info["uf_tribunal"] == uf

    @pytest.mark.parametrize("uf", ["RJ", "ES", "PR", "SC", "RS"])
    def test_eproc_states(self, uf, monkeypatch):
        monkeypatch.setattr(processar_lote_v2, "detect_uf_from_folder", lambda p: uf)
        tipo, info = classify_tribunal("/qualquer/pasta")
        assert tipo == "eproc"
        assert "eproc" in info["sistema"]

    def test_undetected_uf_defaults_to_pje(self, monkeypatch):
        monkeypatch.setattr(processar_lote_v2, "detect_uf_from_folder", lambda p: None)
        assert classify_tribunal("/pasta") == ("pje", {})

    def test_unknown_uf_defaults_to_pje_with_empty_info(self, monkeypatch):
        monkeypatch.setattr(processar_lote_v2, "detect_uf_from_folder", lambda p: "ZZ")
        assert classify_tribunal("/pasta") == ("pje", {})

    def test_returns_trf_in_info(self, monkeypatch):
        monkeypatch.setattr(processar_lote_v2, "detect_uf_from_folder", lambda p: "SP")
        assert classify_tribunal("/pasta")[1]["trf"] == "TRF-3"


class TestValidatePastaPre:
    def test_complete_folder_has_no_issues(self, tmp_path):
        pasta = make_pasta(
            tmp_path, ["1-peticao.pdf", "2-procuracao.pdf", "5-rg.pdf", "7-res.pdf"]
        )
        assert validate_pasta_pre(pasta) == []

    def test_too_few_pdfs_flagged(self, tmp_path):
        pasta = make_pasta(tmp_path, ["2-procuracao.pdf", "5-rg.pdf"])
        assert any("Poucos PDFs" in i for i in validate_pasta_pre(pasta))

    def test_missing_procuracao_flagged(self, tmp_path):
        pasta = make_pasta(tmp_path, ["1-a.pdf", "5-rg.pdf", "6-doc.pdf"])
        assert "Sem procuracao" in validate_pasta_pre(pasta)

    def test_missing_identity_doc_flagged(self, tmp_path):
        pasta = make_pasta(tmp_path, ["1-a.pdf", "2-procuracao.pdf", "9-x.pdf"])
        assert "Sem documento de identidade" in validate_pasta_pre(pasta)

    def test_prefix_6_counts_as_identity(self, tmp_path):
        pasta = make_pasta(tmp_path, ["1-a.pdf", "2-procuracao.pdf", "6-doc.pdf"])
        assert "Sem documento de identidade" not in validate_pasta_pre(pasta)

    def test_procuracao_match_is_case_insensitive(self, tmp_path):
        pasta = make_pasta(tmp_path, ["1-a.pdf", "2-PROCURACAO.pdf", "5-rg.pdf"])
        assert "Sem procuracao" not in validate_pasta_pre(pasta)

    def test_non_pdfs_do_not_count(self, tmp_path):
        pasta = tmp_path / "C"
        pasta.mkdir()
        for fn in ["a.docx", "b.txt", "2-procuracao.pdf"]:
            (pasta / fn).write_bytes(b"x" * 200)
        assert any("Poucos PDFs" in i for i in validate_pasta_pre(str(pasta)))

    def test_empty_folder_reports_all_issues(self, tmp_path):
        pasta = tmp_path / "vazia"
        pasta.mkdir()
        issues = validate_pasta_pre(str(pasta))
        assert len(issues) == 3
