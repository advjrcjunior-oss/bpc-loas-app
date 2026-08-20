import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Must be set before app.py is imported: they keep the background scheduler,
# follow-up and monitor threads from starting during the test run.
os.environ.setdefault("FOLLOWUP_ENABLED", "false")
os.environ.setdefault("MATERNIDADE_ENABLED", "false")
os.environ.setdefault("MONITOR_ENABLED", "false")
os.environ.setdefault("ADMIN_TOKEN", "test-admin-token")

import pytest

MINIMAL_PDF = (
    b"%PDF-1.4\n"
    b"1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
    b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
    b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 612 792]>>endobj\n"
    b"trailer<</Root 1 0 R>>\n"
    b"%%EOF\n"
)


@pytest.fixture
def pdf_file(tmp_path):
    p = tmp_path / "doc.pdf"
    p.write_bytes(MINIMAL_PDF)
    return str(p)


@pytest.fixture
def viacep_success():
    return {
        "cep": "01310-100",
        "logradouro": "Avenida Paulista",
        "bairro": "Bela Vista",
        "localidade": "São Paulo",
        "uf": "SP",
    }


@pytest.fixture
def cpfcnpj_success():
    return {
        "nome": " MARIA DA SILVA ",
        "nascimento": "01/01/1990",
        "sexo": "F",
        "mae": "ANA DA SILVA",
        "logradouro": "Rua das Flores",
        "numero": "100",
        "complemento": "",
        "bairro": "Centro",
        "cidade": "São Paulo",
        "uf": "SP",
        "cep": "01310-100",
        "telefone": "11999998888",
    }
