import os

# Load .env file (only set vars not already in environment)
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                k = k.strip()
                if k not in os.environ:
                    os.environ[k] = v.strip()

# Clean API key (remove leading = or spaces from Railway env)
api_key = os.environ.get("ANTHROPIC_API_KEY", "")
if api_key and not api_key.startswith("sk-"):
    cleaned = api_key.lstrip(" =")
    os.environ["ANTHROPIC_API_KEY"] = cleaned

import re
import json
import glob
import base64
import traceback
import tempfile
from flask import Flask, render_template, request, jsonify, send_from_directory

import shutil
import zipfile
import anthropic
import fitz  # PyMuPDF
from PIL import Image
import openpyxl

# Import all helper functions
import helpers

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
TIMBRADO_PATH = os.path.join(BASE_DIR, "timbrado.docx")
SKILL_PROMPT_PATH = os.path.join(BASE_DIR, "skill_prompt.md")

os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_skill_prompt():
    with open(SKILL_PROMPT_PATH, "r", encoding="utf-8") as f:
        return f.read()


@app.route("/")
def index():
    return render_template("index.html")


def build_exec_globals():
    """Build the exec environment with all helpers pre-loaded."""
    import datetime as dt_module
    import openpyxl as openpyxl_module
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    g = {
        "__builtins__": __builtins__,
        # Paths
        "TIMBRADO_PATH": TIMBRADO_PATH,
        "OUTPUT_DIR": OUTPUT_DIR,
        # Standard modules
        "os": os,
        "re": re,
        "json": json,
        "shutil": shutil,
        "zipfile": zipfile,
        "datetime": dt_module.datetime,  # expose datetime.datetime as "datetime" so datetime.now() works
        "date": dt_module.date,
        "timedelta": dt_module.timedelta,
        "dt_module": dt_module,  # full module available as dt_module if needed
        "Decimal": __import__("decimal").Decimal,
        # openpyxl
        "openpyxl": openpyxl_module,
        "Font": Font,
        "PatternFill": PatternFill,
        "Alignment": Alignment,
        "Border": Border,
        "Side": Side,
        "get_column_letter": get_column_letter,
        # All helper functions
        "esc": helpers.esc,
        "run": helpers.run,
        "para": helpers.para,
        "sec_title": helpers.sec_title,
        "sub_title": helpers.sub_title,
        "bp": helpers.bp,
        "bp_r": helpers.bp_r,
        "ped": helpers.ped,
        "quesito": helpers.quesito,
        "empty": helpers.empty,
        "empty_line": helpers.empty_line,
        "table_row": helpers.table_row,
        "make_table": helpers.make_table,
        "make_total_row": helpers.make_total_row,
        "setup_docx": helpers.setup_docx,
        "save_docx": helpers.save_docx,
        "meses_entre": helpers.meses_entre,
        "data_extenso": helpers.data_extenso,
    }
    return g


@app.route("/api/gerar", methods=["POST"])
def gerar():
    try:
        data = request.get_json()
        client = anthropic.Anthropic()
        skill_prompt = load_skill_prompt()

        # Clean output directory
        for f in glob.glob(os.path.join(OUTPUT_DIR, "*")):
            if os.path.isfile(f) and not f.endswith(".gitkeep"):
                os.remove(f)
            elif os.path.isdir(f):
                shutil.rmtree(f, ignore_errors=True)

        # Determine which documents to generate
        docs = data.get("documentos", ["todos"])
        if "todos" in docs:
            doc_list = ["peticao", "planilha", "quesitos_medicos", "quesitos_sociais"]
        else:
            doc_list = docs

        doc_labels = {
            "peticao": "peticao inicial BPC/LOAS (.docx com timbrado)",
            "planilha": "planilha de calculo de atrasados modelo Conta Facil Prev (.xlsx com openpyxl)",
            "quesitos_medicos": "quesitos para pericia medica (.docx com timbrado)",
            "quesitos_sociais": "quesitos para pericia social (.docx com timbrado)",
        }

        all_files = []
        errors = []
        base_data_msg = build_data_summary(data)

        # Pre-built helper instructions to reduce tokens
        helper_instructions = """
AMBIENTE DE EXECUCAO - as seguintes funcoes e modulos ja estao disponiveis (NAO redefinir):
- Modulos: os, re, json, shutil, zipfile, datetime (classe datetime.datetime, use datetime.now(), datetime.strptime()), date, timedelta, Decimal, openpyxl, Font, PatternFill, Alignment, Border, Side, get_column_letter
- Variaveis: TIMBRADO_PATH, OUTPUT_DIR
- Funcoes DOCX: esc(t), run(text, bold, sz, color, caps, italic), para(runs_str, jc, fi, li, before, after, line, shd, bdr_top, bdr_bot), sec_title(t), sub_title(t), bp(text, fi), bp_r(runs_str, fi, li), ped(letra, texto), quesito(num, texto), empty(), empty_line()
- Funcoes tabela: make_table(headers, rows), make_total_row(cells)
- Funcoes setup: setup_docx(TIMBRADO_PATH, OUTPUT_DIR) -> (base_dir, sect_pr, ns), save_docx(body_xml, sect_pr, ns, base_dir, output_path)
- Funcao calculo: meses_entre(data_str) -> int (meses entre a data e hoje)
- Funcao data: data_extenso() -> str (retorna data atual em portugues: "6 de marco de 2026")

SALARIO MINIMO ATUAL (2026): R$ 1.621,00 (usar SEMPRE este valor em calculos, planilhas e peticoes).
SM 2025 = R$ 1.518,00 (apenas para parcelas vencidas de 2025).
SM 2024 = R$ 1.412,00 (apenas para parcelas vencidas de 2024).

COMO GERAR UM DOCX:
base_dir, sect_pr, ns = setup_docx(TIMBRADO_PATH, OUTPUT_DIR)
body = ""
body += sec_title("TITULO")
body += bp("Paragrafo normal")
save_docx(body, sect_pr, ns, base_dir, os.path.join(OUTPUT_DIR, "arquivo.docx"))

REGRAS DO CODIGO (CRITICAS - seguir rigorosamente):
- NAO redefinir funcoes helper (esc, run, para, bp, etc.) - ja estao carregadas
- NAO usar emojis ou caracteres especiais unicode no codigo
- NAO usar caminhos /mnt/ ou /tmp/ - usar OUTPUT_DIR
- NAO importar modulos que ja estao disponiveis
- Usar apenas ASCII no codigo (acentos sao OK em strings de texto)
- Para nomes em negrito no meio do texto, SEMPRE usar bp_r() com run(): bp_r(run("NOME", bold=True) + run(" resto do texto"))
- NUNCA passar XML como string de texto para bp() - usar bp_r() com multiplos run()
- Datas SEMPRE em portugues: usar data_extenso() para data atual
- PLANILHA: usar SM 2026 = 1621.00 para parcelas vincendas e valor da causa. Para parcelas vencidas, usar o SM do ano correspondente.
- PETICAO: valor da causa = (meses_vencidos * SM_do_ano) + (12 * 1621.00). Usar SM 2026 = R$ 1.621,00.
- NUNCA usar f-strings aninhadas com aspas iguais. Exemplo ERRADO: f"{f'algo'}". Usar variaveis intermediarias.
- Testar que todas as variaveis existem antes de usar em f-strings
- TERMINOLOGIA: usar "autor" (masculino) ou "autora" (feminino) em vez de "requerente". NUNCA usar "requerente".
- Nos quesitos, usar "Autor:" ou "Autora:" na qualificacao resumida.
"""

        for doc_type in doc_list:
            label = doc_labels.get(doc_type, doc_type)
            print(f"[GERANDO] {label}...")

            user_msg = f"""Gere APENAS: {label}

{base_data_msg}

{helper_instructions}

Gere um UNICO bloco de codigo Python. NAO gere outros documentos.
"""

            last_error = None
            for attempt in range(3):  # up to 3 attempts
                try:
                    if attempt > 0 and last_error:
                        user_msg_attempt = user_msg + f"\n\nERRO NA TENTATIVA ANTERIOR: {last_error}\nCorrija o erro e gere novamente. Verifique parenteses, f-strings e sintaxe.\n"
                        print(f"[RETRY {attempt+1}] {label}: {last_error[:100]}")
                    else:
                        user_msg_attempt = user_msg

                    response_text = ""
                    with client.messages.stream(
                        model="claude-sonnet-4-20250514",
                        max_tokens=16000,
                        system=skill_prompt,
                        messages=[{"role": "user", "content": user_msg_attempt}],
                    ) as stream:
                        for text in stream.text_stream:
                            response_text += text

                    code = extract_python_code(response_text)
                    if not code:
                        last_error = "Claude nao gerou codigo Python"
                        continue

                    # Sanitize code
                    code = sanitize_code(code)

                    # Execute with pre-loaded helpers
                    exec_globals = build_exec_globals()
                    old_cwd = os.getcwd()
                    os.chdir(BASE_DIR)
                    try:
                        exec(code, exec_globals)
                    finally:
                        os.chdir(old_cwd)

                    # Check generated files
                    for f in os.listdir(OUTPUT_DIR):
                        if f.endswith((".docx", ".xlsx")) and not f.startswith("_") and f not in all_files:
                            all_files.append(f)

                    print(f"[OK] {label}")
                    last_error = None
                    break

                except Exception as e:
                    traceback.print_exc()
                    last_error = str(e)

            if last_error:
                errors.append(f"{label}: {last_error}")

        if not all_files:
            error_msg = "Nenhum documento foi gerado."
            if errors:
                error_msg += " Erros: " + "; ".join(errors)
            return jsonify({"error": error_msg}), 400

        result = {"files": sorted(all_files), "message": f"{len(all_files)} documento(s) gerado(s) com sucesso!"}
        if errors:
            result["warnings"] = errors
        return jsonify(result)

    except anthropic.APIError as e:
        return jsonify({"error": f"Erro na API do Claude: {str(e)}"}), 500
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"Erro: {str(e)}"}), 500


@app.route("/api/lote", methods=["POST"])
def lote():
    """Process multiple client folders end-to-end.
    For each folder: analyze docs → extract data → generate all documents → copy to folder.
    Returns results via streaming SSE so frontend can show progress.
    """
    from flask import Response, stream_with_context
    data = request.get_json()
    pastas = data.get("pastas", [])

    def generate():
        results = []
        for i, pasta in enumerate(pastas):
            pasta = pasta.strip()
            if not pasta or not os.path.isdir(pasta):
                yield f"data: {json.dumps({'type': 'error', 'pasta': pasta, 'message': 'Pasta nao encontrada'})}\n\n"
                continue

            cliente_nome = os.path.basename(pasta).split("_")[0].strip()
            yield f"data: {json.dumps({'type': 'progress', 'pasta': pasta, 'step': 'analisando', 'index': i, 'total': len(pastas), 'cliente': cliente_nome})}\n\n"

            # Step 1: Analyze folder
            try:
                extracted = analisar_pasta_internal(pasta)
            except Exception as e:
                traceback.print_exc()
                yield f"data: {json.dumps({'type': 'error', 'pasta': pasta, 'cliente': cliente_nome, 'message': f'Erro na analise: {str(e)}'})}\n\n"
                continue

            yield f"data: {json.dumps({'type': 'progress', 'pasta': pasta, 'step': 'gerando', 'index': i, 'total': len(pastas), 'cliente': cliente_nome})}\n\n"

            # Step 2: Generate documents
            try:
                generated_files = gerar_documentos_internal(extracted)
            except Exception as e:
                traceback.print_exc()
                yield f"data: {json.dumps({'type': 'error', 'pasta': pasta, 'cliente': cliente_nome, 'message': f'Erro na geracao: {str(e)}'})}\n\n"
                continue

            # Step 3: Remove old generated docs from client folder before copying new ones
            generated_patterns = ["peticao_bpc", "calculo_atrasados", "quesitos_pericia",
                                  "PETICAO INICIAL", "CALCULO DE ATRASADOS", "QUESITOS PERICIA",
                                  "1- PETICAO", "17- CALCULO", "18- QUESITOS", "19- QUESITOS"]
            for f in os.listdir(pasta):
                f_path = os.path.join(pasta, f)
                if os.path.isfile(f_path):
                    f_lower = f.lower()
                    if any(p.lower() in f_lower for p in generated_patterns):
                        try:
                            os.remove(f_path)
                            print(f"  [REMOVIDO] {f} (antigo)")
                        except Exception:
                            pass

            # Copy new generated files
            copied = []
            for fname in generated_files:
                src = os.path.join(OUTPUT_DIR, fname)
                dst = os.path.join(pasta, fname)
                try:
                    shutil.copy2(src, dst)
                    copied.append(fname)
                except Exception as e:
                    print(f"[WARN] Erro ao copiar {fname}: {e}")

            # Step 5: Detect duplicates, organize, merge multi-part PDFs, convert xlsx to PDF
            yield f"data: {json.dumps({'type': 'progress', 'pasta': pasta, 'step': 'organizando', 'index': i, 'total': len(pastas), 'cliente': cliente_nome})}\n\n"
            try:
                detect_duplicates(pasta)
            except Exception as e:
                print(f"[WARN] Erro ao detectar duplicados: {e}")
            try:
                merge_pdf_parts(pasta)  # First pass: merge original "parte1/parte2" files
            except Exception as e:
                print(f"[WARN] Erro ao juntar PDFs: {e}")
            try:
                organizar_pasta(pasta)
            except Exception as e:
                print(f"[WARN] Erro ao organizar pasta: {e}")
            try:
                merge_pdf_parts(pasta)  # Second pass: merge renamed "3- CONTRATO 1/2/3" files
            except Exception as e:
                print(f"[WARN] Erro ao juntar PDFs renomeados: {e}")

            # Convert xlsx to PDF (after organizing, so file has final name)
            for f in os.listdir(pasta):
                if f.endswith('.xlsx') and os.path.isfile(os.path.join(pasta, f)):
                    pdf_result = xlsx_to_pdf(os.path.join(pasta, f))
                    if pdf_result:
                        copied.append(os.path.basename(pdf_result))

            yield f"data: {json.dumps({'type': 'done', 'pasta': pasta, 'cliente': cliente_nome, 'files': copied, 'index': i, 'total': len(pastas)})}\n\n"
            results.append({"pasta": pasta, "cliente": cliente_nome, "files": copied})

        yield f"data: {json.dumps({'type': 'complete', 'results': results})}\n\n"

    return Response(stream_with_context(generate()), mimetype='text/event-stream')


def analisar_pasta_internal(pasta):
    """Internal: analyze a folder and return extracted data dict.

    Handles large folders by:
    1. Extracting text from ALL PDFs first (cheap, small)
    2. Only sending images for scanned docs that lack text
    3. Tracking total base64 size to stay under API limits (~9MB safe)
    4. Reducing DPI for large folders
    5. Skipping non-essential docs (contrato, procuracao, OAB) as images
    """
    extensions = (".pdf", ".png", ".jpg", ".jpeg", ".bmp", ".tiff", ".webp")
    files_found = []
    for f in sorted(os.listdir(pasta)):
        if f.lower().endswith(extensions) and not f.startswith('.') and f != 'desktop.ini':
            files_found.append(os.path.join(pasta, f))

    if not files_found:
        raise ValueError("Nenhum PDF ou imagem encontrado na pasta.")

    print(f"[ANALISE] {len(files_found)} arquivos em {os.path.basename(pasta)}")

    # Extract text from all PDFs
    file_info = []
    for filepath in files_found:
        filename = os.path.basename(filepath)
        info = {"path": filepath, "name": filename, "text": "", "pages": 0, "has_text": False}
        if filepath.lower().endswith(".pdf"):
            info["type"] = "pdf"
            try:
                doc = fitz.open(filepath)
                info["pages"] = len(doc)
                text = ""
                for page in doc:
                    text += page.get_text() + "\n"
                doc.close()
                info["text"] = text.strip()
                info["has_text"] = len(info["text"]) > 100
            except Exception:
                pass
        else:
            info["type"] = "image"
        file_info.append(info)

    # Build content
    text_files = [f for f in file_info if f["has_text"]]
    scan_files = [f for f in file_info if not f["has_text"]]

    # Truncate very long text to avoid huge requests (e.g. PROCESSO INSS with 50+ pages)
    MAX_TEXT_PER_FILE = 8000  # chars
    text_content = f"Analise os documentos de um cliente BPC/LOAS.\nTotal: {len(file_info)} arquivos.\n\n"
    for f in text_files:
        file_text = f['text']
        if len(file_text) > MAX_TEXT_PER_FILE:
            file_text = file_text[:MAX_TEXT_PER_FILE] + f"\n[... texto truncado, total {len(f['text'])} chars ...]"
        text_content += f"=== {f['name']} ({f['pages']}pg) ===\n{file_text}\n\n"

    content_parts = [{"type": "text", "text": text_content}]

    # Scanned docs as images - with size tracking
    def scan_priority(f):
        name = f["name"].lower()
        for i, kw in enumerate(["laudo", "certid", "nascimento", "rg", "cpf", "identif", "sus", "autodecl", "cadunico", "relatorio", "parecer", "receita", "encaminhamento", "grupo_familiar", "comprometimento"]):
            if kw in name:
                return i
        return 99

    # Skip non-essential docs as images (we already got their text if available)
    SKIP_IMAGE_KEYWORDS = ["contrato", "procura", "oab", "pedido.gratuidade", "fatura", "biometria", "conta.luz"]

    scan_files.sort(key=scan_priority)
    total_images = 0
    total_b64_bytes = 0
    MAX_IMAGES = 15
    MAX_B64_BYTES = 9 * 1024 * 1024  # 9MB safe limit
    DPI_SCAN = 120  # lower DPI for large folders

    for f in scan_files:
        if total_images >= MAX_IMAGES or total_b64_bytes >= MAX_B64_BYTES:
            break

        # Skip non-essential docs as images
        name_lower = f["name"].lower()
        if any(kw in name_lower for kw in SKIP_IMAGE_KEYWORDS):
            print(f"  [SKIP IMG] {f['name']} (nao essencial)")
            continue

        content_parts.append({"type": "text", "text": f"\n--- Escaneado: {f['name']} ---"})
        if f["type"] == "pdf":
            try:
                max_pg = min(2, MAX_IMAGES - total_images)
                images = pdf_to_images(f["path"], max_pages=max_pg, dpi=DPI_SCAN)
                for img_b64 in images:
                    b64_size = len(img_b64)
                    if total_b64_bytes + b64_size > MAX_B64_BYTES:
                        break
                    content_parts.append({"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": img_b64}})
                    total_images += 1
                    total_b64_bytes += b64_size
            except Exception:
                pass
        else:
            try:
                img_b64, mt = image_to_base64(f["path"])
                if img_b64:
                    b64_size = len(img_b64)
                    if total_b64_bytes + b64_size <= MAX_B64_BYTES:
                        content_parts.append({"type": "image", "source": {"type": "base64", "media_type": mt, "data": img_b64}})
                        total_images += 1
                        total_b64_bytes += b64_size
            except Exception:
                pass

    print(f"[ANALISE] Enviando: {len(text_files)} docs com texto + {total_images} imagens ({total_b64_bytes / 1024 / 1024:.1f}MB)")

    content_parts.append({"type": "text", "text": EXTRACTION_PROMPT})

    client = anthropic.Anthropic()
    response_text = ""
    with client.messages.stream(
        model="claude-haiku-4-5-20251001",
        max_tokens=4096,
        messages=[{"role": "user", "content": content_parts}],
    ) as stream:
        for text in stream.text_stream:
            response_text += text

    extracted = extract_json(response_text)
    if not extracted:
        raise ValueError("Nao conseguiu extrair dados dos documentos")

    return extracted


def gerar_documentos_internal(data):
    """Internal: generate all 4 documents from extracted data. Returns list of filenames."""
    client = anthropic.Anthropic()
    skill_prompt = load_skill_prompt()

    # Clean output
    for f in glob.glob(os.path.join(OUTPUT_DIR, "*")):
        if os.path.isfile(f) and not f.endswith(".gitkeep"):
            os.remove(f)
        elif os.path.isdir(f):
            shutil.rmtree(f, ignore_errors=True)

    doc_list = [
        ("peticao", "peticao inicial BPC/LOAS (.docx com timbrado)"),
        ("planilha", "planilha de calculo de atrasados modelo Conta Facil Prev (.xlsx com openpyxl)"),
        ("quesitos_medicos", "quesitos para pericia medica (.docx com timbrado)"),
        ("quesitos_sociais", "quesitos para pericia social (.docx com timbrado)"),
    ]

    base_data_msg = build_data_summary(data)
    helper_instructions = """
AMBIENTE DE EXECUCAO - funcoes e modulos ja disponiveis (NAO redefinir):
- Modulos: os, re, json, shutil, zipfile, datetime (classe datetime.datetime, use datetime.now(), datetime.strptime()), date, timedelta, Decimal, openpyxl, Font, PatternFill, Alignment, Border, Side, get_column_letter
- Variaveis: TIMBRADO_PATH, OUTPUT_DIR
- Funcoes DOCX: esc(t), run(text, bold, sz, color, caps, italic), para(runs_str, jc, fi, li, before, after, line, shd, bdr_top, bdr_bot), sec_title(t), sub_title(t), bp(text, fi), bp_r(runs_str, fi, li), ped(letra, texto), quesito(num, texto), empty(), empty_line()
- Funcoes tabela: make_table(headers, rows), make_total_row(cells)
- Funcoes setup: setup_docx(TIMBRADO_PATH, OUTPUT_DIR) -> (base_dir, sect_pr, ns), save_docx(body_xml, sect_pr, ns, base_dir, output_path)
- Funcao calculo: meses_entre(data_str) -> int
- Funcao data: data_extenso() -> str (data atual em portugues)

SALARIO MINIMO ATUAL (2026): R$ 1.621,00 (usar SEMPRE este valor em calculos, planilhas e peticoes).
SM 2025 = R$ 1.518,00 (apenas para parcelas vencidas de 2025). SM 2024 = R$ 1.412,00.

COMO GERAR DOCX:
base_dir, sect_pr, ns = setup_docx(TIMBRADO_PATH, OUTPUT_DIR)
body = ""
body += sec_title("TITULO")
body += bp("Paragrafo")
save_docx(body, sect_pr, ns, base_dir, os.path.join(OUTPUT_DIR, "arquivo.docx"))

REGRAS CRITICAS:
- NAO redefinir helpers. NAO usar emojis. NAO usar /mnt/ ou /tmp/. Usar OUTPUT_DIR.
- Para nomes em negrito: bp_r(run("NOME", bold=True) + run(" resto"))
- NUNCA passar XML como texto para bp(). Datas em portugues com data_extenso().
- PLANILHA: SM 2026 = 1621.00 para vincendas. Para vencidas usar SM do ano correspondente.
- NUNCA usar f-strings aninhadas. Usar variaveis intermediarias.
- TERMINOLOGIA: usar "autor" (masculino) ou "autora" (feminino) em vez de "requerente". NUNCA usar "requerente".
- Nos quesitos, usar "Autor:" ou "Autora:" na qualificacao resumida.
"""

    all_files = []

    for doc_type, label in doc_list:
        print(f"  [GERANDO] {label}...")
        user_msg = f"Gere APENAS: {label}\n\n{base_data_msg}\n\n{helper_instructions}\n\nGere um UNICO bloco ```python. NAO gere outros documentos."

        last_error = None
        for attempt in range(3):
            try:
                msg = user_msg
                if attempt > 0 and last_error:
                    msg += f"\n\nERRO ANTERIOR: {last_error}\nCorrija e gere novamente."
                    print(f"  [RETRY {attempt+1}] {last_error[:80]}")

                response_text = ""
                with client.messages.stream(
                    model="claude-sonnet-4-20250514",
                    max_tokens=16000,
                    system=[{"type": "text", "text": skill_prompt, "cache_control": {"type": "ephemeral"}}],
                    messages=[{"role": "user", "content": msg}],
                    extra_headers={"anthropic-beta": "prompt-caching-2024-07-31"},
                ) as stream:
                    for text in stream.text_stream:
                        response_text += text

                code = extract_python_code(response_text)
                if not code:
                    last_error = "Sem codigo Python"
                    continue

                code = sanitize_code(code)
                exec_globals = build_exec_globals()
                old_cwd = os.getcwd()
                os.chdir(BASE_DIR)
                try:
                    exec(code, exec_globals)
                finally:
                    os.chdir(old_cwd)

                for f in os.listdir(OUTPUT_DIR):
                    if f.endswith((".docx", ".xlsx")) and not f.startswith("_") and f not in all_files:
                        all_files.append(f)

                print(f"  [OK] {label}")
                last_error = None
                break
            except Exception as e:
                traceback.print_exc()
                last_error = str(e)

        if last_error:
            print(f"  [FALHOU] {label}: {last_error}")

    return all_files


@app.route("/api/analisar-pasta", methods=["POST"])
def analisar_pasta():
    """Analyze documents in a folder and extract client data.

    Strategy for handling large folders:
    1. Extract text from ALL PDFs first (text is tiny vs images)
    2. Classify each file: has_text (>100 chars) or scanned (needs image)
    3. Send all text-based content in one message
    4. For scanned docs, send images in batches if needed
    5. If a second batch is needed, merge results
    """
    try:
        data = request.get_json()
        pasta = data.get("pasta", "").strip()

        if not pasta or not os.path.isdir(pasta):
            return jsonify({"error": f"Pasta nao encontrada: {pasta}"}), 400

        # Collect all PDFs and images
        extensions = (".pdf", ".png", ".jpg", ".jpeg", ".bmp", ".tiff", ".webp")
        files_found = []
        for f in sorted(os.listdir(pasta)):
            if f.lower().endswith(extensions) and not f.startswith('.') and f != 'desktop.ini':
                files_found.append(os.path.join(pasta, f))

        if not files_found:
            return jsonify({"error": "Nenhum PDF ou imagem encontrado na pasta."}), 400

        print(f"[ANALISE] {len(files_found)} arquivos encontrados em {pasta}")

        # === PASS 1: Extract text from all PDFs ===
        file_info = []  # list of {path, name, type, text, pages, has_text}
        for filepath in files_found:
            filename = os.path.basename(filepath)
            info = {"path": filepath, "name": filename, "text": "", "pages": 0, "has_text": False}

            if filepath.lower().endswith(".pdf"):
                info["type"] = "pdf"
                try:
                    doc = fitz.open(filepath)
                    info["pages"] = len(doc)
                    text = ""
                    for page in doc:
                        text += page.get_text() + "\n"
                    doc.close()
                    info["text"] = text.strip()
                    info["has_text"] = len(info["text"]) > 100
                except Exception as e:
                    print(f"[WARN] Erro ao ler {filename}: {e}")
            else:
                info["type"] = "image"
                info["has_text"] = False

            file_info.append(info)
            print(f"  [{info['type'].upper()}] {filename} - {info['pages']}pg - texto: {len(info['text'])} chars")

        # === PASS 2: Build content for Claude ===
        # Separate files into text-based and image-needed
        text_files = [f for f in file_info if f["has_text"]]
        scan_files = [f for f in file_info if not f["has_text"]]

        # Build the text-only content first (all PDFs with extractable text)
        text_content = f"Analise os documentos abaixo de um cliente BPC/LOAS e extraia todos os dados.\n\n"
        text_content += f"Total de arquivos: {len(file_info)}\n\n"

        for f in text_files:
            text_content += f"=== {f['name']} ({f['pages']} paginas) ===\n"
            text_content += f"{f['text']}\n\n"

        # Build multimodal parts: text summary + scanned images
        content_parts = [{"type": "text", "text": text_content}]

        # Add scanned documents as images (with size control)
        # Target: stay under ~18MB total to avoid 413
        MAX_IMAGES = 20  # conservative limit
        total_images = 0
        DPI_SCAN = 150   # good quality but reasonable size

        # Prioritize scanned files: laudos and key docs first
        def scan_priority(f):
            name = f["name"].lower()
            if "laudo" in name:
                return 0
            if "certid" in name or "nascimento" in name:
                return 1
            if "rg" in name or "cpf" in name or "identif" in name:
                return 2
            if "sus" in name:
                return 3
            if "autodecl" in name or "cadunico" in name:
                return 4
            return 5

        scan_files.sort(key=scan_priority)

        for f in scan_files:
            if total_images >= MAX_IMAGES:
                content_parts.append({
                    "type": "text",
                    "text": f"\n[AVISO: {f['name']} nao incluido como imagem - limite atingido. "
                            f"Se houver texto extraido, ja foi incluido acima.]"
                })
                continue

            content_parts.append({
                "type": "text",
                "text": f"\n--- Documento escaneado: {f['name']} ---"
            })

            if f["type"] == "pdf":
                # Scanned PDF: convert pages to images
                max_pg = min(3, MAX_IMAGES - total_images)
                try:
                    images = pdf_to_images(f["path"], max_pages=max_pg, dpi=DPI_SCAN)
                    for img_b64 in images:
                        content_parts.append({
                            "type": "image",
                            "source": {"type": "base64", "media_type": "image/png", "data": img_b64}
                        })
                        total_images += 1
                except Exception as e:
                    print(f"[WARN] Erro ao converter {f['name']} para imagem: {e}")
            else:
                # Direct image file
                try:
                    img_b64, media_type = image_to_base64(f["path"])
                    if img_b64:
                        content_parts.append({
                            "type": "image",
                            "source": {"type": "base64", "media_type": media_type, "data": img_b64}
                        })
                        total_images += 1
                except Exception as e:
                    print(f"[WARN] Erro ao processar imagem {f['name']}: {e}")

        print(f"[ANALISE] Enviando: {len(text_files)} docs com texto + {total_images} imagens")

        # Add extraction prompt
        content_parts.append({
            "type": "text",
            "text": EXTRACTION_PROMPT
        })

        # === PASS 3: Call Claude API with streaming ===
        client = anthropic.Anthropic()
        response_text = ""
        with client.messages.stream(
            model="claude-haiku-4-5-20251001",
            max_tokens=4096,
            messages=[{"role": "user", "content": content_parts}],
        ) as stream:
            for text in stream.text_stream:
                response_text += text

        # Parse JSON response
        extracted = extract_json(response_text)
        if not extracted:
            return jsonify({"error": "Nao foi possivel extrair dados. Resposta:\n" + response_text[:2000]}), 400

        return jsonify({
            "data": extracted,
            "arquivos_analisados": [f["name"] for f in file_info],
            "message": f"{len(file_info)} documento(s) analisado(s) com sucesso!"
        })

    except anthropic.APIError as e:
        return jsonify({"error": f"Erro na API do Claude: {str(e)}"}), 500
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"Erro: {str(e)}"}), 500


# Extraction prompt constant
EXTRACTION_PROMPT = """
Com base em TODOS os documentos acima, extraia e retorne APENAS um JSON valido (sem markdown, sem ```), com esta estrutura exata:

{
  "nome": "NOME COMPLETO DO REQUERENTE (crianca/pessoa com deficiencia)",
  "cpf": "000.000.000-00",
  "rg": "",
  "data_nascimento": "YYYY-MM-DD",
  "endereco": "endereco completo com rua, numero, bairro, CEP",
  "cidade": "cidade",
  "estado": "UF (sigla de 2 letras)",
  "representante_nome": "nome completo do representante legal",
  "representante_cpf": "CPF do representante",
  "representante_rg": "RG do representante",
  "representante_parentesco": "mae/pai/tutor/curador",
  "cids": "F90.0, F84.0 (todos os CIDs encontrados nos laudos)",
  "descricao_saude": "descricao DETALHADA: diagnosticos, sintomas, limitacoes funcionais, medicamentos em uso, tratamentos necessarios. Extrair dos laudos medicos.",
  "nb": "numero do beneficio (formato XXX.XXX.XXX-X)",
  "der": "YYYY-MM-DD (data de entrada do requerimento)",
  "motivo_indeferimento": "motivo exato do indeferimento pelo INSS",
  "familia": [
    {"nome": "NOME COMPLETO", "parentesco": "mae/pai/irmao/irma/avo", "idade": "XX", "renda": "0,00"}
  ],
  "gastos": [
    {"categoria": "Saude", "item": "nome do gasto", "valor": "", "observacao": "Art. 20-B LOAS"}
  ]
}

REGRAS:
- Se um dado nao foi encontrado, deixe como string vazia ""
- Para familia, incluir TODOS os membros que aparecem nos documentos (CadUnico, certidoes, etc.)
- Para gastos de saude (medicamentos, terapias, consultas), SEMPRE colocar observacao "Art. 20-B LOAS"
- O valor dos gastos pode ficar vazio se nao encontrado nos documentos
- Extrair o MAXIMO de informacoes possiveis
- Retornar APENAS o JSON, sem texto antes ou depois
"""


def pdf_extract_text(pdf_path):
    """Extract text from all pages of a PDF."""
    text = ""
    try:
        doc = fitz.open(pdf_path)
        for page in doc:
            text += page.get_text() + "\n"
        doc.close()
    except Exception:
        pass
    return text


def pdf_to_images(pdf_path, max_pages=20, dpi=200):
    """Convert PDF pages to base64 PNG images."""
    images = []
    doc = fitz.open(pdf_path)
    for i, page in enumerate(doc):
        if i >= max_pages:
            break
        mat = fitz.Matrix(dpi / 72, dpi / 72)
        pix = page.get_pixmap(matrix=mat)
        img_bytes = pix.tobytes("png")
        images.append(base64.b64encode(img_bytes).decode("utf-8"))
    doc.close()
    return images


def image_to_base64(image_path):
    """Convert image file to base64 with appropriate media type."""
    ext = os.path.splitext(image_path)[1].lower()
    media_types = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".bmp": "image/bmp",
        ".tiff": "image/tiff",
        ".webp": "image/webp",
    }
    media_type = media_types.get(ext, "image/png")

    # Resize if too large (Claude has limits)
    try:
        img = Image.open(image_path)
        max_dim = 2048
        if img.width > max_dim or img.height > max_dim:
            img.thumbnail((max_dim, max_dim), Image.LANCZOS)

        # Convert to PNG for consistency
        import io
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        img_bytes = buf.getvalue()
        return base64.b64encode(img_bytes).decode("utf-8"), "image/png"
    except Exception:
        # Fallback: read raw bytes
        with open(image_path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8"), media_type


def extract_json(text):
    """Extract JSON object from Claude's response."""
    # Try direct parse
    text = text.strip()
    if text.startswith("{"):
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

    # Try to find JSON block in markdown
    match = re.search(r"```(?:json)?\s*\n(\{.*?\})\s*\n```", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass

    # Try to find any JSON object
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    return None


@app.route("/api/download/<path:filename>")
def download(filename):
    return send_from_directory(OUTPUT_DIR, filename, as_attachment=True)


def build_data_summary(data):
    """Build data summary for individual document generation."""
    msg = f"""DADOS DO CLIENTE:
- Nome: {data.get('nome', '')}
- CPF: {data.get('cpf', '')}
- Data de nascimento: {data.get('data_nascimento', '')}
- RG: {data.get('rg', '')}
- Endereço: {data.get('endereco', '')}
- Cidade/Comarca: {data.get('cidade', '')}
- Estado: {data.get('estado', '')}
"""

    if data.get("representante_nome"):
        msg += f"""
REPRESENTANTE LEGAL:
- Nome: {data.get('representante_nome', '')}
- CPF: {data.get('representante_cpf', '')}
- RG: {data.get('representante_rg', '')}
- Parentesco: {data.get('representante_parentesco', '')}
"""

    msg += f"""
CONDIÇÃO DE SAÚDE:
- CID(s): {data.get('cids', '')}
- Descrição da condição: {data.get('descricao_saude', '')}

DADOS DO PROCESSO:
- NB (Número do Benefício): {data.get('nb', '')}
- DER (Data de Entrada do Requerimento): {data.get('der', '')}
- Motivo do indeferimento: {data.get('motivo_indeferimento', '')}
"""

    # Composição familiar
    familia = data.get("familia", [])
    if familia:
        msg += "\nCOMPOSIÇÃO FAMILIAR:\n"
        for m in familia:
            msg += f"- {m.get('nome', '')}, {m.get('parentesco', '')}, idade: {m.get('idade', '')}, renda: R$ {m.get('renda', '0,00')}\n"

    # Gastos
    gastos = data.get("gastos", [])
    if gastos:
        msg += "\nGASTOS MENSAIS:\n"
        for g in gastos:
            obs = f" ({g.get('observacao', '')})" if g.get("observacao") else ""
            msg += f"- {g.get('categoria', '')}: {g.get('item', '')} - R$ {g.get('valor', '0,00')}{obs}\n"

    return msg


def sanitize_code(code):
    """Clean up generated code to avoid common errors."""
    # Remove emoji/unicode chars that break Windows charmap
    code = re.sub(r'[\U00010000-\U0010ffff]', '', code)
    code = code.replace('\u2713', 'v').replace('\u2705', '[OK]').replace('\u274c', '[X]')
    code = code.replace('\u2022', '-').replace('\u2014', '-').replace('\u2013', '-')
    code = code.replace('\u2018', "'").replace('\u2019', "'")
    code = code.replace('\u201c', '"').replace('\u201d', '"')
    code = code.replace('\u2026', '...')
    code = code.replace('\u00a0', ' ')  # non-breaking space

    # Remove lines that try to redefine our helpers (common source of errors)
    lines = code.split('\n')
    filtered = []
    skip_until_unindent = False
    skip_func_name = None

    helper_funcs = {'esc', 'run', 'para', 'sec_title', 'sub_title', 'bp', 'bp_r',
                    'ped', 'quesito', 'empty', 'empty_line', 'table_row',
                    'make_table', 'make_total_row', 'setup_docx', 'save_docx', 'meses_entre'}

    for line in lines:
        stripped = line.strip()
        # Skip helper function redefinitions
        if stripped.startswith('def '):
            func_name = stripped.split('(')[0].replace('def ', '').strip()
            if func_name in helper_funcs:
                skip_until_unindent = True
                skip_func_name = func_name
                continue
        if skip_until_unindent:
            if stripped == '' or line[0:1] in (' ', '\t'):
                continue
            else:
                skip_until_unindent = False
                skip_func_name = None

        # Remove redundant imports of already-loaded modules
        if stripped.startswith('import ') and any(m in stripped for m in
            ['import shutil', 'import zipfile', 'import re', 'import os',
             'import json', 'import openpyxl', 'from openpyxl']):
            continue
        if stripped.startswith('from datetime import'):
            continue

        filtered.append(line)

    return '\n'.join(filtered)


def xlsx_to_pdf(xlsx_path):
    """Convert xlsx to PDF using Excel COM automation."""
    try:
        import subprocess
        abs_path = os.path.abspath(xlsx_path).replace('/', '\\')
        pdf_path = os.path.splitext(abs_path)[0] + '.pdf'
        # Write PS script to temp file to avoid encoding issues
        import tempfile
        ps_file = os.path.join(tempfile.gettempdir(), '_xlsx2pdf.ps1')
        with open(ps_file, 'w', encoding='utf-8-sig') as pf:
            pf.write(f'''
$ErrorActionPreference = "Stop"
$excel = New-Object -ComObject Excel.Application
$excel.Visible = $false
$excel.DisplayAlerts = $false
try {{
    $wb = $excel.Workbooks.Open("{abs_path}")
    $ws = $wb.Worksheets.Item(1)
    $ws.PageSetup.Orientation = 2
    $ws.PageSetup.FitToPagesWide = 1
    $ws.PageSetup.FitToPagesTall = $false
    $ws.PageSetup.LeftMargin = $excel.InchesToPoints(0.4)
    $ws.PageSetup.RightMargin = $excel.InchesToPoints(0.4)
    $ws.PageSetup.TopMargin = $excel.InchesToPoints(0.5)
    $ws.PageSetup.BottomMargin = $excel.InchesToPoints(0.5)
    $wb.ExportAsFixedFormat(0, "{pdf_path}")
    $wb.Close($false)
    Write-Output "OK"
}} catch {{
    Write-Output ("ERRO: " + $_.Exception.Message)
}} finally {{
    $excel.Quit()
    [System.Runtime.Interopservices.Marshal]::ReleaseComObject($excel) | Out-Null
}}
''')
        result = subprocess.run(
            ['powershell', '-ExecutionPolicy', 'Bypass', '-File', ps_file],
            capture_output=True, timeout=60)
        stdout = result.stdout.decode('utf-8', errors='replace').strip()
        print(f"  [xlsx_to_pdf] stdout: {stdout}")
        if 'OK' in stdout:
            print(f"  [PDF] {os.path.basename(pdf_path)} gerado")
            return pdf_path
        else:
            stderr = result.stderr.decode('utf-8', errors='replace').strip()
            print(f"  [WARN] xlsx_to_pdf stderr: {stderr}")
    except Exception as e:
        print(f"  [WARN] xlsx_to_pdf falhou: {e}")
    return None


def merge_pdf_parts(pasta):
    """Find and merge multi-part PDFs (e.g. contrato.parte1.pdf, contrato.parte2.pdf)."""
    files = [f for f in os.listdir(pasta) if f.lower().endswith('.pdf') and os.path.isfile(os.path.join(pasta, f))]

    # Group files by base name (detect patterns like "name.parte1.pdf", "name_part1.pdf",
    # "03_contrato_1.pdf", "3- CONTRATO DE HONORARIOS 1.pdf")
    groups = {}
    for f in files:
        f_lower = f.lower()
        # Pattern 1: name.parte1.pdf / name.parte2.pdf
        match = re.match(r'^(.+?)[\._\s]*(parte|part)[\._\s]*(\d+)\.pdf$', f_lower)
        if match:
            base = match.group(1).rstrip('._- ')
            num = int(match.group(3))
            groups.setdefault(base, []).append((num, f))
            continue
        # Pattern 2: "3- CONTRATO DE HONORARIOS 1.pdf" / "03_contrato_1.pdf"
        match = re.match(r'^(\d+[-_]\s*.+?)\s+(\d+)\.pdf$', f_lower)
        if match:
            base = match.group(1).rstrip()
            num = int(match.group(2))
            groups.setdefault(base, []).append((num, f))
            continue

    # Only merge groups that are clearly multi-part documents (contrato, relatorio desenvolvimental)
    # Do NOT merge procuracoes, documentos de ID, certidoes etc. (these are separate docs)
    MERGE_KEYWORDS = ["contrato", "relatorio desenvolvimental", "relatorio.desenvolvimental"]
    SKIP_MERGE = ["procura", "certid", "doc. de ident", "documento do", "doc_menor",
                  "doc_responsavel", "comprovante", "quesitos", "peticao", "cadunico",
                  "laudo", "receita", "exame", "fatura"]

    merged = []
    for base, parts in groups.items():
        if len(parts) < 2:
            continue
        # Check if this group should be merged
        base_lower = base.lower()
        should_merge = any(kw in base_lower for kw in MERGE_KEYWORDS)
        should_skip = any(kw in base_lower for kw in SKIP_MERGE)
        if should_skip or not should_merge:
            continue
        parts.sort(key=lambda x: x[0])
        print(f"  [MERGE] Juntando {len(parts)} partes: {base}")

        try:
            merged_doc = fitz.open()
            for num, fname in parts:
                pdf_path = os.path.join(pasta, fname)
                doc = fitz.open(pdf_path)
                merged_doc.insert_pdf(doc)
                doc.close()

            # Save merged file - use uppercase label from first file if available
            first_file = parts[0][1]
            base_from_file = re.match(r'^(.+?)\s+\d+\.pdf$', first_file, re.IGNORECASE)
            if base_from_file:
                merged_name = base_from_file.group(1).strip() + '.pdf'
            else:
                merged_name = base.replace('.', ' ').strip() + '.pdf'
            merged_path = os.path.join(pasta, merged_name)
            merged_doc.save(merged_path)
            merged_doc.close()
            print(f"  [MERGE] Salvo: {merged_name}")

            # Move parts to subfolder
            parts_dir = os.path.join(pasta, '_partes_originais')
            os.makedirs(parts_dir, exist_ok=True)
            for num, fname in parts:
                src = os.path.join(pasta, fname)
                dst = os.path.join(parts_dir, fname)
                if os.path.exists(src):
                    shutil.move(src, dst)

            merged.append(merged_name)
        except Exception as e:
            print(f"  [WARN] Erro ao juntar {base}: {e}")

    return merged


def detect_duplicates(pasta):
    """Detect duplicate files by content hash and move to subfolder."""
    import hashlib

    files = [f for f in os.listdir(pasta)
             if os.path.isfile(os.path.join(pasta, f)) and not f.startswith('.') and f != 'desktop.ini']

    # Hash all files
    hashes = {}
    for f in files:
        path = os.path.join(pasta, f)
        try:
            with open(path, 'rb') as fh:
                h = hashlib.md5(fh.read()).hexdigest()
            hashes.setdefault(h, []).append(f)
        except Exception:
            pass

    # Find duplicates
    duplicates = []
    dup_dir = os.path.join(pasta, '_duplicados')
    for h, fnames in hashes.items():
        if len(fnames) > 1:
            # Keep the first (alphabetically), move the rest
            fnames.sort()
            keep = fnames[0]
            for dup in fnames[1:]:
                if not os.path.isdir(dup_dir):
                    os.makedirs(dup_dir, exist_ok=True)
                src = os.path.join(pasta, dup)
                dst = os.path.join(dup_dir, dup)
                try:
                    shutil.move(src, dst)
                    duplicates.append((dup, keep))
                    print(f"  [DUPLICADO] {dup} (igual a {keep}) -> _duplicados/")
                except Exception as e:
                    print(f"  [WARN] Erro ao mover duplicado {dup}: {e}")

    return duplicates


def organizar_pasta(pasta):
    """Organize files in client folder with numeric prefixes for easy filing order."""
    # Order categories with keywords to match filenames
    ORDEM = [
        (1, "PETICAO INICIAL", ["peticao_bpc", "peticao_inicial", "01_peticao"]),
        (2, "PROCURACAO", ["procura", "02_procuracao"]),
        (3, "CONTRATO DE HONORARIOS", ["contrato", "03_contrato", "CONTRATO DE HONORARIOS"]),
        (4, "DECLARACAO DE HIPOSSUFICIENCIA", ["pedido.gratuidade", "hipossufici", "declaracao_hipo", "gratuidade", "04_declaracao"]),
        (5, "DOCUMENTO DO MENOR", ["DOC. DE IDENTIFICAÇÃO-CERTIDÃO DE NASCIMENTO.pdf",
                             "ID HEITOR", "ID ESTHEFANY", "ID MAYCON", "ID ADRIAN",
                             "certid", "05_doc_menor", "5- DOCUMENTO DO MENOR"]),
        (6, "DOCUMENTO DO RESPONSAVEL", ["DOC. DE IDENTIFICAÇÃO-RG-REPRESENTANTE", "rg.representante", "rg.mae", "rg.pai",
                                   "DOC. DE IDENTIFICAÇÃO-RG-PAI",
                                   "ID JULIANA", "ID PATRICIA", "ID LARISSA", "ID RESPONSAVEL",
                                   "06_doc_responsavel", "6- DOCUMENTO DO RESPONSAVEL"]),
        (7, "COMPROVANTE DE RESIDENCIA", ["conta.luz", "comprovante.resid", "comprov.resid",
                                          "COMPROVANTE DE RESIDENCIA", "autodecl",
                                          "07_comprovante_residencia", "7- COMPROVANTE DE RESIDENCIA"]),
        (8, "CADUNICO", ["cadunico", "CADUNICO", "08_cadunico"]),
        (9, "DOCUMENTOS DO GRUPO FAMILIAR", ["grupo_familiar", "DOC. DE IDENTIFICAÇÃO-CERTIDÃO DE NASCIMENTO-IRM",
                                       "DOC. DE IDENTIFICAÇÃO-CPF-IRM", "certidao.nascimento.irm", "09_docs"]),
        (10, "COMPROVANTE DE RENDA", ["comprometimento_renda", "comprovante.renda", "extrato", "10_comprovante_renda"]),
        (11, "REQUERIMENTO INSS", ["PROCESSO INSS", "requerimento", "protocolo.inss", "11_requerimento"]),
        (12, "CARTA DE INDEFERIMENTO", ["indeferimento", "carta.inss"]),
        (13, "LAUDO MEDICO", ["laudo", "LAUDO", "13_laudo"]),
        (14, "RELATORIO MEDICO", ["relatorio", "RELATÓRIO", "parecer", "14_relatorio"]),
        (15, "RECEITAS E EXAMES", ["receita", "exame", "encaminhamento", "ENCAMINHAMENTO",
                                   "COMPROVANTE DE AGENDAMENTO", "COMPROVANTE-LISTA", "GUIA",
                                   "15_receitas"]),
        (16, "COMPROVANTE DE GASTOS", ["fatura", "FATURA", "nota.fiscal", "comprovante.gasto", "16_comprovante"]),
        (17, "CALCULO DE ATRASADOS", ["calculo_atrasados", "17_calculo"]),
        (18, "QUESITOS PERICIA MEDICA", ["quesitos_pericia_medica", "quesitos_medic", "18_quesitos_medic"]),
        (19, "QUESITOS PERICIA SOCIAL", ["quesitos_pericia_social", "quesitos_soci", "19_quesitos_soci"]),
        (20, "BIOMETRIA", ["biometria", "BIOMETRIA"]),
        (21, "OAB", ["OAB (", "oab_", "carteira.oab"]),
    ]

    # Files to skip
    SKIP = ["desktop.ini", ".gitkeep"]

    files = [f for f in os.listdir(pasta)
             if os.path.isfile(os.path.join(pasta, f)) and f not in SKIP and not f.startswith('.')]

    renamed = []
    used_files = set()

    # Pre-match files already renamed with "N- LABEL" pattern
    already_renamed_re = re.compile(r'^(\d+)-\s+(.+?)(?:\s+\d+)?(\.[^.]+)$')

    for num, label, keywords in ORDEM:
        matches = []
        for f in files:
            if f in used_files:
                continue
            # Check if already renamed with correct prefix
            m = already_renamed_re.match(f)
            if m and int(m.group(1)) == num:
                used_files.add(f)  # Already correctly named, skip
                continue
            f_lower = f.lower()
            for kw in keywords:
                if kw.lower() in f_lower:
                    matches.append(f)
                    break

        for idx, f in enumerate(sorted(matches)):
            used_files.add(f)
            ext = os.path.splitext(f)[1]
            if len(matches) > 1:
                new_name = f"{num}- {label} {idx+1}{ext}"
            else:
                new_name = f"{num}- {label}{ext}"
            src = os.path.join(pasta, f)
            dst = os.path.join(pasta, new_name)
            try:
                if src != dst:
                    if os.path.exists(dst):
                        counter = 2
                        base, ext2 = os.path.splitext(new_name)
                        while os.path.exists(os.path.join(pasta, f"{base} {counter}{ext2}")):
                            counter += 1
                        new_name = f"{base} {counter}{ext2}"
                        dst = os.path.join(pasta, new_name)
                    os.rename(src, dst)
                    renamed.append((f, new_name))
                    print(f"  [RENOMEADO] {f} -> {new_name}")
            except Exception as e:
                print(f"  [WARN] Erro ao renomear {f}: {e}")

    # Files that didn't match any category - leave them as is
    unmatched = [f for f in files if f not in used_files]
    if unmatched:
        print(f"  [INFO] Arquivos nao categorizados: {', '.join(unmatched)}")

    return renamed


def extract_python_code(text):
    """Extract Python code block from Claude's response."""
    # Try ```python ... ``` first (greedy to get the largest block)
    match = re.search(r"```python\s*\n(.*?)```", text, re.DOTALL)
    if match:
        return match.group(1).strip()

    # Try ``` ... ```
    match = re.search(r"```\s*\n(.*?)```", text, re.DOTALL)
    if match:
        code = match.group(1).strip()
        if "import" in code or "def " in code:
            return code

    # If code block was truncated (no closing ```), extract everything after ```python
    match = re.search(r"```python\s*\n(.*)", text, re.DOTALL)
    if match:
        code = match.group(1).strip()
        # Remove trailing ``` if present at the very end
        code = re.sub(r"```\s*$", "", code).strip()
        if "import" in code:
            return code

    return None


# ==================== LEGALMAIL INTEGRATION ====================

LEGALMAIL_API_KEY = os.environ.get("LEGALMAIL_API_KEY", "")
LEGALMAIL_CERT_ID = int(os.environ.get("LEGALMAIL_CERTIFICADO_ID", "0"))
LEGALMAIL_BASE = "https://app.legalmail.com.br/api/v1"

# Map organized file prefixes to LegalMail attachment type IDs
# Map file prefix -> attachment type NAME (resolved to ID dynamically per tribunal)
LEGALMAIL_DOC_TYPE_NAMES = {
    "2": "Procuração",
    "3": "Contrato de Honorários",
    "4": "Declaração de Hipossuficiência",
    "5": "Certidão de Nascimento",
    "6": "Identidade",
    "7": "Comprovante de Residência",
    "8": "Comprovantes",
    "9": "Identidade",
    "10": "Comprovantes",
    "11": "Comprovantes",
    "12": "Carta de Indeferimento",
    "13": "Laudo",
    "14": "Laudo",
    "15": "Receituário",
    "16": "Comprovantes",
    "17": "Planilha",
    "18": "Quesitos Perícia",
    "19": "Quesitos Perícia",
    "20": "Identidade",
    "21": "Identidade",
}

# Cache: petition_id -> {type_name_lower: type_id}
_attachment_type_cache = {}


def legalmail_resolve_doc_type(idpeticoes, prefix):
    """Get the correct attachment type ID for a file prefix, fetching from API if needed."""
    global _attachment_type_cache

    if idpeticoes not in _attachment_type_cache:
        r = legalmail_request("get", f"/petition/attachment/types?idpeticoes={idpeticoes}")
        if r.status_code != 200:
            print(f"  [WARN] Falha ao buscar tipos de anexo: {r.status_code}")
            return None
        types = r.json() if r.text.strip().startswith('[') else []
        type_map = {}
        for t in types:
            name = t.get('nome', '').strip()
            tid = str(t.get('iddocumentos_tipos', ''))
            type_map[name.lower()] = tid
        _attachment_type_cache[idpeticoes] = type_map

    type_map = _attachment_type_cache[idpeticoes]
    target_name = LEGALMAIL_DOC_TYPE_NAMES.get(prefix, "Outros")
    target_lower = target_name.lower()

    # Exact match
    if target_lower in type_map:
        return type_map[target_lower]

    # Partial match: target in name or name in target
    for name, tid in type_map.items():
        if target_lower in name or name in target_lower:
            return tid

    # Broader fallback: match first word
    first_word = target_lower.split()[0] if target_lower else ""
    for name, tid in type_map.items():
        if first_word and first_word in name:
            return tid

    # Last resort: "Outros" or "Comprovantes"
    return type_map.get('outros') or type_map.get('comprovantes')

# BPC/LOAS defaults per system
# ATENÇÃO: NUNCA classificar como benefício previdenciário por incapacidade (remessa indevida à Central de Perícias)
BPC_DEFAULTS = {
    'eproc': {
        'rito': 'RITO ORDINÁRIO (COMUM)',
        'classe': 'PROCEDIMENTO COMUM',
        'competencia': 'Federal',
        'assunto_search': 'Benefício Assistencial',
    },
    'pje': {
        'rito': 'JUIZADO ESPECIAL FEDERAL',
        'classe': 'PROCEDIMENTO DO JUIZADO ESPECIAL CÍVEL (436)',
        'area': 'DIREITO PREVIDENCIÁRIO',
        'assunto_search': 'Pessoa com Deficiência (11946)',
    },
}
# eProc TRF-4: classes endpoint returns EMPTY - classe/assunto must be filled manually in LegalMail
# Legacy constant kept for reference
LEGALMAIL_ASSUNTO_BPC_PJE = "DIREITO ADMINISTRATIVO E OUTRAS MATÉRIAS DE DIREITO PÚBLICO (9985) | Garantias Constitucionais (9986) | Assistência Social (11847)"

# INSS data (polo passivo) - fixed for all BPC cases
INSS_DATA = {
    "nome": "INSTITUTO NACIONAL DO SEGURO SOCIAL - INSS",
    "polo": "passivo",
    "documento": "29.979.036/0001-40",
    "endereco_cep": "70040-902",
    "endereco_logradouro": "SAUS Quadra 2 Bloco O",
    "endereco_numero": "S/N",
    "endereco_bairro": "Asa Sul",
    "endereco_cidade": "Brasilia",
    "endereco_uf": "DF",
}
# Cached INSS party ID (set on first use)
_inss_party_id = None


def _parse_brl(val):
    """Parse a BRL value string or number to float. Handles 'R$ 29.231,70' and '29231.70'."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return round(float(val), 2)
    s = str(val).strip()
    s = s.replace('R$', '').replace(' ', '').strip()
    if not s:
        return None
    # Detect format: "29.231,70" (BR) vs "29231.70" (US) vs "29,231.70" (US with commas)
    if ',' in s and '.' in s:
        if s.rindex(',') > s.rindex('.'):
            # BR format: 29.231,70
            s = s.replace('.', '').replace(',', '.')
        else:
            # US format: 29,231.70
            s = s.replace(',', '')
    elif ',' in s and '.' not in s:
        # Could be "29231,70" (BR decimal) or "29,231" (US thousands)
        # If comma is near end (1-2 digits after), treat as decimal
        parts = s.split(',')
        if len(parts[-1]) <= 2:
            s = s.replace(',', '.')
        else:
            s = s.replace(',', '')
    try:
        return round(float(s), 2)
    except ValueError:
        return None


def extract_valor_causa(pasta):
    """Extract valor da causa from the xlsx spreadsheet in client folder."""
    try:
        import openpyxl
        xlsx_files = [f for f in os.listdir(pasta) if f.endswith('.xlsx') and 'CALCULO' in f.upper()]
        if not xlsx_files:
            return None
        xlsx_path = os.path.join(pasta, xlsx_files[0])
        wb = openpyxl.load_workbook(xlsx_path, data_only=True)
        ws = wb.active

        # Strategy 1: Find "TOTAL DA CONTA" (not SUBTOTAL) - check embedded and columns
        total_candidates = []
        for row in ws.iter_rows(min_row=1, max_row=30, max_col=6, values_only=False):
            for cell in row:
                if cell.value and isinstance(cell.value, str):
                    text_upper = cell.value.upper()
                    # Match "TOTAL DA CONTA" but NOT "SUBTOTAL"
                    if 'TOTAL DA CONTA' in text_upper and 'SUBTOTAL' not in text_upper:
                        # Check embedded value in same cell
                        if 'R$' in cell.value:
                            val = _parse_brl(cell.value.split('R$')[-1])
                            if val and val > 100:
                                total_candidates.append(val)
                        # Check columns B-E of same row
                        for col in [5, 4, 3, 2]:
                            val = _parse_brl(ws.cell(row=cell.row, column=col).value)
                            if val and val > 100:
                                total_candidates.append(val)

        if total_candidates:
            wb.close()
            return max(total_candidates)  # Take the largest (total > subtotal)

        # Strategy 2: Find any "TOTAL" cell with embedded R$ value (skip SUBTOTAL)
        for row in ws.iter_rows(min_row=1, max_row=30, max_col=6, values_only=False):
            for cell in row:
                if cell.value and isinstance(cell.value, str):
                    text_upper = cell.value.upper()
                    if 'TOTAL' in text_upper and 'SUBTOTAL' not in text_upper and 'R$' in cell.value:
                        val = _parse_brl(cell.value.split('R$')[-1])
                        if val and val > 100:
                            wb.close()
                            return val

        # Strategy 3: Check common positions (E13, E12, E11, B11, B12)
        for r, c in [(13, 5), (12, 5), (11, 5), (12, 2), (11, 2)]:
            val = _parse_brl(ws.cell(row=r, column=c).value)
            if val and val > 100:
                wb.close()
                return val

        # Strategy 4: Scan all cells for the largest monetary value (likely total)
        max_val = None
        for row in ws.iter_rows(min_row=1, max_row=20, max_col=6, values_only=False):
            for cell in row:
                val = _parse_brl(cell.value)
                if val and val > 100:
                    if max_val is None or val > max_val:
                        max_val = val
        wb.close()
        return max_val

    except Exception as e:
        print(f"  [WARN] extract_valor_causa: {e}")
        try:
            wb.close()
        except Exception:
            pass
    return None


def legalmail_get_or_create_inss():
    """Find or create INSS party on LegalMail. Returns party ID."""
    global _inss_party_id
    if _inss_party_id is not None:
        return _inss_party_id

    import requests
    # Search existing parties for INSS
    try:
        r = legalmail_request("get", "/parts")
        if r.status_code == 200:
            parts = r.json()
            for p in parts:
                if '29.979.036' in str(p.get('documento', '')) or 'INSS' in str(p.get('nome', '')):
                    _inss_party_id = int(p.get('id'))
                    print(f"  [LEGALMAIL] INSS encontrado: id={_inss_party_id}")
                    return _inss_party_id
    except Exception:
        pass

    # Create INSS party
    r = legalmail_request("post", "/parts", json=INSS_DATA)
    if r.status_code == 200:
        data = r.json()
        _inss_party_id = data.get('id')
        print(f"  [LEGALMAIL] INSS criado: id={_inss_party_id}")
        return _inss_party_id
    else:
        print(f"  [WARN] Falha ao criar INSS: {r.status_code} {r.text[:200]}")
    return None


def legalmail_create_party(party_data):
    """Create a party on LegalMail. Returns party ID (int) or None."""
    r = legalmail_request("post", "/parts", json=party_data)
    if r.status_code == 200:
        data = r.json()
        raw_id = data.get('id')
        if raw_id is None:
            print(f"  [WARN] API retornou sem 'id': {data}")
            return None
        pid = int(raw_id)
        print(f"  [LEGALMAIL] Parte criada: {party_data.get('nome', '?')} id={pid}")
        return pid
    else:
        print(f"  [WARN] Falha ao criar parte: {r.status_code} {r.text[:200]}")
    return None


def legalmail_find_party_by_doc(documento):
    """Find existing party by CPF/CNPJ. Returns party ID or None."""
    try:
        r = legalmail_request("get", "/parts")
        if r.status_code == 200:
            parts = r.json()
            doc_clean = documento.replace('.', '').replace('-', '').replace('/', '')
            for p in parts:
                p_doc = str(p.get('documento', '')).replace('.', '').replace('-', '').replace('/', '')
                if p_doc == doc_clean:
                    return p.get('id')
    except Exception:
        pass
    return None


def legalmail_fill_fields(idpeticoes, sistema, comarca_name, valor_causa=None,
                           id_polo_ativo=None, id_polo_passivo=None,
                           tipo_beneficio='deficiente'):
    """Fill all petition fields step by step following API dependency chain.

    Flow: comarca -> rito -> competencia -> classe -> assunto -> flags/valor -> parties
    tipo_beneficio: 'deficiente' or 'idoso' (for eProc assunto selection)
    Returns dict with status.
    """
    import time
    filled = {}
    errors = []

    def safe_put(payload, label):
        r = legalmail_request("put", f"/petition/initial?idpeticoes={idpeticoes}", json=payload)
        if r.status_code == 200:
            print(f"  [LEGALMAIL] {label}: OK")
            filled[label] = True
        elif r.status_code == 429:
            print(f"  [LEGALMAIL] Rate limit, aguardando 60s...")
            time.sleep(60)
            r = legalmail_request("put", f"/petition/initial?idpeticoes={idpeticoes}", json=payload)
            if r.status_code == 200:
                print(f"  [LEGALMAIL] {label}: OK (retry)")
                filled[label] = True
            else:
                errors.append(f"{label}: {r.status_code} {r.text[:150]}")
                print(f"  [WARN] {label}: {r.status_code}")
        else:
            errors.append(f"{label}: {r.status_code} {r.text[:150]}")
            print(f"  [WARN] {label}: {r.status_code} - {r.text[:150]}")
        return r

    def safe_get(endpoint, label):
        r = legalmail_request("get", endpoint)
        if r.status_code == 429:
            print(f"  [LEGALMAIL] Rate limit em {label}, aguardando 60s...")
            time.sleep(60)
            r = legalmail_request("get", endpoint)
        if r.status_code == 200 and r.text.strip().startswith('['):
            return r.json()
        return []

    is_eproc = 'eproc' in sistema.lower() if sistema else False
    defaults = BPC_DEFAULTS.get('eproc' if is_eproc else 'pje', {})

    # Step 1: Set competencia + comarca
    # PJe: competencia='DIREITO PREVIDENCIÁRIO', some TRFs need both in same PUT
    # eProc: competencia='Federal' or area='Cível', needs comarca+rito first
    comp_ok = False
    comarca_ok = False

    if is_eproc:
        # eProc: comarca first, then rito, then competencia
        if comarca_name:
            safe_put({'comarca': comarca_name}, 'comarca')
            comarca_ok = 'comarca' in filled
            time.sleep(2)
    else:
        # PJe: try competencia alone first
        area = defaults.get('area', 'DIREITO PREVIDENCIÁRIO')
        safe_put({'competencia': area}, f'competencia ({area})')
        comp_ok = any('competencia' in k for k in filled)
        time.sleep(2)

        if comp_ok and comarca_name:
            safe_put({'comarca': comarca_name}, 'comarca')
            comarca_ok = 'comarca' in filled
            time.sleep(2)
        elif comarca_name:
            # Some PJe TRFs need competencia+comarca in same PUT (e.g. TRF-1)
            safe_put({'competencia': area, 'comarca': comarca_name}, f'comp+comarca ({area})')
            if f'comp+comarca ({area})' in filled:
                comp_ok = True
                comarca_ok = True
            time.sleep(2)

    # Step 2: Set rito
    rito_target = defaults.get('rito', 'JUIZADO ESPECIAL FEDERAL')
    ritos = safe_get(f"/petition/ritos?idpeticoes={idpeticoes}", 'ritos')
    if ritos:
        match = [r for r in ritos if r.get('nome', '').upper() == rito_target.upper()]
        if not match:
            keyword = 'ORDINÁRIO' if 'ORDINÁRIO' in rito_target.upper() else 'JUIZADO'
            match = [r for r in ritos if keyword in r.get('nome', '').upper()]
        rito_name = match[0]['nome'] if match else ritos[0]['nome']
        safe_put({'rito': rito_name}, f'rito ({rito_name})')
        time.sleep(2)

    # Step 2b: eProc - retry competencia after rito (needs comarca+rito first)
    if is_eproc and not comp_ok:
        safe_put({'competencia': 'Federal'}, 'competencia (Federal)')
        comp_ok = any('competencia' in k for k in filled)
        time.sleep(2)

    # Step 3: Set classe
    classe_target = defaults.get('classe', 'PROCEDIMENTO COMUM')
    classe_set = False
    classes = safe_get(f"/petition/classes?idpeticoes={idpeticoes}", 'classes')
    if classes:
        match = [c for c in classes if classe_target.upper() in c.get('nome', '').upper()]
        if not match:
            match = [c for c in classes if 'procedimento comum' in c.get('nome', '').lower()]
        if match:
            cls_name = match[0]['nome']
            safe_put({'classe': cls_name}, f'classe ({cls_name})')
            classe_set = any('classe' in k for k in filled)
            time.sleep(2)

    # Step 4: Set assunto (BPC - NUNCA usar benefício previdenciário por incapacidade)
    assunto_set = False
    subjects = safe_get(f"/petition/subjects?idpeticoes={idpeticoes}", 'subjects')
    if subjects:
        # Search for BPC Deficiente or Idoso
        search_term = 'defici' if tipo_beneficio == 'deficiente' else 'idoso'
        # Find exact BPC match: "Benefício Assistencial (Art. 203,V CF/88) > Pessoa com Deficiência"
        for s in subjects:
            nome = s.get('nome', '')
            if 'assistencial' in nome.lower() and '203' in nome and search_term in nome.lower():
                safe_put({'assunto': nome}, f'assunto ({nome[:60]})')
                assunto_set = any('assunto' in k for k in filled)
                break
        if not assunto_set:
            # Broader search
            for s in subjects:
                nome = s.get('nome', '')
                if 'assistencial' in nome.lower() and search_term in nome.lower():
                    safe_put({'assunto': nome}, f'assunto ({nome[:60]})')
                    assunto_set = any('assunto' in k for k in filled)
                    break
        time.sleep(2)

    # Step 6: Set flags and valor da causa
    flags_payload = {
        'gratuidade': True,
        'liminar': True,
        '100digital': True,
        'renuncia60Salarios': True,
        'distribuicao': 'Por sorteio',
    }
    if valor_causa:
        flags_payload['valorCausa'] = valor_causa
    safe_put(flags_payload, 'flags + valorCausa')
    time.sleep(2)

    # Step 7: Set parties
    parties_payload = {}
    if id_polo_ativo:
        parties_payload['idpoloativo'] = [id_polo_ativo] if isinstance(id_polo_ativo, int) else id_polo_ativo
    if id_polo_passivo:
        parties_payload['idpolopassivo'] = [id_polo_passivo] if isinstance(id_polo_passivo, int) else id_polo_passivo
    if parties_payload:
        safe_put(parties_payload, 'partes')

    return {"filled": filled, "errors": errors}


def docx_to_pdf(docx_path):
    """Convert docx to PDF using Word COM automation."""
    try:
        import subprocess
        abs_path = os.path.abspath(docx_path).replace('/', '\\')
        pdf_path = os.path.splitext(abs_path)[0] + '.pdf'
        import tempfile
        ps_file = os.path.join(tempfile.gettempdir(), '_docx2pdf.ps1')
        with open(ps_file, 'w', encoding='utf-8-sig') as pf:
            pf.write(f'''
$ErrorActionPreference = "Stop"
$word = New-Object -ComObject Word.Application
$word.Visible = $false
$word.DisplayAlerts = 0
try {{
    $doc = $word.Documents.Open("{abs_path}")
    $doc.SaveAs2("{pdf_path}", 17)
    $doc.Close($false)
    Write-Output "OK"
}} catch {{
    Write-Output ("ERRO: " + $_.Exception.Message)
}} finally {{
    $word.Quit()
    [System.Runtime.Interopservices.Marshal]::ReleaseComObject($word) | Out-Null
}}
''')
        result = subprocess.run(
            ['powershell', '-ExecutionPolicy', 'Bypass', '-File', ps_file],
            capture_output=True, timeout=60)
        stdout = result.stdout.decode('utf-8', errors='replace').strip()
        if 'OK' in stdout:
            print(f"  [PDF] {os.path.basename(pdf_path)} gerado (docx)")
            return pdf_path
        else:
            print(f"  [WARN] docx_to_pdf: {stdout}")
    except Exception as e:
        print(f"  [WARN] docx_to_pdf falhou: {e}")
    return None


def legalmail_request(method, endpoint, **kwargs):
    """Make authenticated request to LegalMail API."""
    import requests
    sep = '&' if '?' in endpoint else '?'
    url = f"{LEGALMAIL_BASE}{endpoint}{sep}api_key={LEGALMAIL_API_KEY}"
    resp = getattr(requests, method)(url, **kwargs, timeout=30)
    return resp


def legalmail_criar_rascunho(pasta, tribunal, sistema, comarca, assunto=None,
                             instancia="1", uf_tribunal="", client_data=None):
    """Create a draft petition on LegalMail with ALL fields filled and documents uploaded.

    Args:
        pasta: client folder path (organized with numbered files)
        tribunal: tribunal code (e.g. "TRF-4", "TRT-9")
        sistema: system code (e.g. "eproc_jfpr", "pje", "pje_trt")
        comarca: comarca name (e.g. "Curitiba", "Guarulhos")
        assunto: subject string (optional, auto-detected from API)
        instancia: "1" or "2" (default "1")
        uf_tribunal: state code when needed (e.g. "SP" for TRF-3/pje)
        client_data: optional dict with polo ativo party data:
            {nome, documento (CPF), endereco_cep, endereco_logradouro,
             endereco_numero, endereco_bairro, endereco_cidade, endereco_uf}

    Returns:
        dict with idpeticoes, idprocessos, status, filled fields, etc.
    """
    import requests, time

    if not LEGALMAIL_API_KEY:
        return {"error": "LEGALMAIL_API_KEY não configurada"}

    # Step 1: Create petition initial (draft)
    print(f"  [LEGALMAIL] Criando rascunho: {tribunal}/{sistema} - {comarca}...")
    payload = {
        "tribunal": tribunal,
        "instancia": instancia,
        "sistema": sistema,
        "certificado_id": LEGALMAIL_CERT_ID,
    }
    if uf_tribunal:
        payload["ufTribunal"] = uf_tribunal

    resp = legalmail_request("post", "/petition/initial", json=payload)
    if resp.status_code == 429:
        print("  [LEGALMAIL] Rate limit, aguardando 60s...")
        time.sleep(60)
        resp = legalmail_request("post", "/petition/initial", json=payload)
    if resp.status_code != 200:
        return {"status": "erro", "error": f"HTTP {resp.status_code}: {resp.text[:200]}"}
    try:
        data = resp.json()
    except Exception:
        return {"status": "erro", "error": f"Resposta inválida: {resp.text[:200]}"}
    if isinstance(data, list):
        data = data[0]
    if data.get("status") != "sucesso":
        return {"status": "erro", "error": f"Erro ao criar petição: {data}"}

    dados = data.get("dados", {})
    idpeticoes = dados.get("idpeticoes")
    idprocessos = dados.get("idprocessos")
    if not idpeticoes:
        return {"status": "erro", "error": f"ID da petição não retornado: {data}"}
    print(f"  [LEGALMAIL] Rascunho criado: idpeticoes={idpeticoes}")

    # Step 2: Extract valor da causa from xlsx
    valor_causa = extract_valor_causa(pasta)
    if valor_causa:
        print(f"  [LEGALMAIL] Valor da causa extraído: R$ {valor_causa:,.2f}")
    else:
        print(f"  [WARN] Valor da causa não encontrado no xlsx")

    # Step 3: Create/find parties
    id_polo_ativo = None
    id_polo_passivo = None

    # INSS (polo passivo)
    time.sleep(2)
    id_polo_passivo = legalmail_get_or_create_inss()

    # Polo ativo (client) - POST /parts is idempotent (same CPF returns same ID)
    if client_data and client_data.get('documento'):
        time.sleep(2)
        party_data = {**client_data, 'polo': 'ativo'}
        # Include etnia for tribunals that require it (TRF-2, etc.)
        if 'etnia' not in party_data:
            party_data['etnia'] = 'Não declarada'
        id_polo_ativo = legalmail_create_party(party_data)

    # Step 4: Fill all petition fields (comarca -> rito -> competencia -> classe -> assunto -> flags -> parties)
    time.sleep(2)
    fill_result = legalmail_fill_fields(
        idpeticoes, sistema, comarca,
        valor_causa=valor_causa,
        id_polo_ativo=id_polo_ativo,
        id_polo_passivo=id_polo_passivo,
    )
    print(f"  [LEGALMAIL] Campos preenchidos: {list(fill_result['filled'].keys())}")
    if fill_result['errors']:
        for err in fill_result['errors']:
            print(f"  [WARN] {err}")

    # Step 5: Collect and process files from organized folder (sorted by numeric prefix)
    all_files = [f for f in os.listdir(pasta)
                 if os.path.isfile(os.path.join(pasta, f)) and not f.startswith('_')]

    def sort_key(fname):
        m = re.match(r'^(\d+)-', fname)
        return (int(m.group(1)), fname) if m else (9999, fname)

    files = sorted(all_files, key=sort_key)

    peticao_pdf = None
    attachments = []

    for f in files:
        filepath = os.path.join(pasta, f)
        match = re.match(r'^(\d+)-\s+', f)
        if not match:
            continue
        prefix = match.group(1)

        if prefix == "1":
            if f.endswith('.docx'):
                peticao_pdf = docx_to_pdf(filepath)
            elif f.endswith('.pdf'):
                peticao_pdf = filepath
        else:
            # Resolve doc type dynamically from API (per tribunal)
            doc_type_id = legalmail_resolve_doc_type(idpeticoes, prefix)
            if doc_type_id:
                if f.endswith('.docx'):
                    pdf_path = docx_to_pdf(filepath)
                    if pdf_path:
                        attachments.append((pdf_path, doc_type_id, f))
                elif f.endswith('.pdf'):
                    attachments.append((filepath, doc_type_id, f))
                elif f.endswith('.xlsx'):
                    pdf_version = os.path.splitext(filepath)[0] + '.pdf'
                    if os.path.exists(pdf_version):
                        continue
                    pdf_path = xlsx_to_pdf(filepath)
                    if pdf_path:
                        attachments.append((pdf_path, doc_type_id, f))
            else:
                print(f"  [WARN] Tipo de anexo não encontrado para prefixo {prefix}: {f}")

    # Step 6: Upload main petition PDF
    time.sleep(2)
    if peticao_pdf and os.path.exists(peticao_pdf):
        print(f"  [LEGALMAIL] Enviando petição principal...")
        with open(peticao_pdf, 'rb') as pf:
            resp = legalmail_request("post", f"/petition/file?idpeticoes={idpeticoes}&idprocessos={idprocessos}",
                files={"file": (os.path.basename(peticao_pdf), pf, "application/pdf")})
        if resp.status_code == 429:
            print(f"  [LEGALMAIL] Rate limit na petição, aguardando 60s...")
            time.sleep(60)
            with open(peticao_pdf, 'rb') as pf:
                resp = legalmail_request("post", f"/petition/file?idpeticoes={idpeticoes}&idprocessos={idprocessos}",
                    files={"file": (os.path.basename(peticao_pdf), pf, "application/pdf")})
        print(f"  [LEGALMAIL] Petição enviada: {resp.status_code}")
    else:
        print(f"  [WARN] Petição PDF não encontrada/gerada")

    # Step 7: Upload attachments
    uploaded = 0
    for filepath, doc_type_id, original_name in attachments:
        try:
            time.sleep(1)
            with open(filepath, 'rb') as af:
                resp = legalmail_request("post",
                    f"/petition/attachments?idpeticoes={idpeticoes}&fk_documentos_tipos={doc_type_id}",
                    files={"file": (os.path.basename(filepath), af, "application/pdf")})
            if resp.status_code == 200:
                uploaded += 1
                print(f"  [LEGALMAIL] Anexo enviado: {original_name}")
            elif resp.status_code == 429:
                print(f"  [LEGALMAIL] Rate limit, aguardando 60s...")
                time.sleep(60)
                with open(filepath, 'rb') as af:
                    resp = legalmail_request("post",
                        f"/petition/attachments?idpeticoes={idpeticoes}&fk_documentos_tipos={doc_type_id}",
                        files={"file": (os.path.basename(filepath), af, "application/pdf")})
                if resp.status_code == 200:
                    uploaded += 1
                    print(f"  [LEGALMAIL] Anexo enviado (retry): {original_name}")
                else:
                    print(f"  [WARN] Erro ao enviar {original_name} (retry): {resp.status_code} {resp.text[:100]}")
            else:
                print(f"  [WARN] Erro ao enviar {original_name}: {resp.status_code} {resp.text[:100]}")
        except Exception as e:
            print(f"  [WARN] Erro ao enviar {original_name}: {e}")

    print(f"  [LEGALMAIL] Rascunho completo: {uploaded} anexos, campos={list(fill_result['filled'].keys())}")

    return {
        "status": "ok",
        "idpeticoes": idpeticoes,
        "idprocessos": idprocessos,
        "uploaded_attachments": uploaded,
        "filled_fields": list(fill_result['filled'].keys()),
        "fill_errors": fill_result['errors'],
        "valor_causa": valor_causa,
        "polo_ativo_id": id_polo_ativo,
        "polo_passivo_id": id_polo_passivo,
        "url": f"https://app.legalmail.com.br/petitions/{idpeticoes}"
    }


@app.route("/api/debug/key", methods=["GET"])
def debug_key():
    key = LEGALMAIL_API_KEY
    return jsonify({"key_length": len(key), "key_preview": key[:8] + "..." + key[-4:] if key else "VAZIO"})


@app.route("/api/legalmail/tribunais", methods=["GET"])
def legalmail_tribunais():
    """List all available tribunals and their systems."""
    resp = legalmail_request("get", "/petition/tribunals")
    return jsonify(resp.json())


@app.route("/api/legalmail/comarcas", methods=["GET"])
def legalmail_comarcas():
    """List available comarcas for a given tribunal/sistema."""
    tribunal = request.args.get("tribunal", "TRF-4")
    sistema = request.args.get("sistema", "eproc_jfpr")
    uf_tribunal = request.args.get("uf", "")

    # Create temp petition to get comarcas
    payload = {"tribunal": tribunal, "instancia": "1", "sistema": sistema,
               "certificado_id": LEGALMAIL_CERT_ID, "gratuidade": True}
    if uf_tribunal:
        payload["ufTribunal"] = uf_tribunal
    resp = legalmail_request("post", "/petition/initial", json=payload)
    data = resp.json()
    if isinstance(data, list):
        data = data[0]
    if data.get("status") != "sucesso":
        return jsonify({"error": str(data)}), 400
    idpeticoes = data["dados"]["idpeticoes"]

    # Get comarcas, subjects, classes
    resp_comarcas = legalmail_request("get", f"/petition/county?idpeticoes={idpeticoes}")
    resp_assuntos = legalmail_request("get", f"/petition/subjects?idpeticoes={idpeticoes}")

    result = {
        "comarcas": resp_comarcas.json(),
        "assuntos": resp_assuntos.json(),
    }

    # Delete temp petition
    legalmail_request("delete", f"/petition/initial?idpeticoes={idpeticoes}")

    return jsonify(result)


@app.route("/api/legalmail/rascunho", methods=["POST"])
def legalmail_rascunho():
    """Create a draft petition on LegalMail for a client folder."""
    data = request.get_json()
    pasta = data.get("pasta", "").strip()
    tribunal = data.get("tribunal", "TRF-4").strip()
    sistema = data.get("sistema", "eproc_jfpr").strip()
    comarca = data.get("comarca", "").strip()
    assunto = data.get("assunto", "").strip() or None
    uf_tribunal = data.get("uf", "").strip()
    client_data = data.get("client_data")  # optional: {nome, documento, endereco_*}

    if not pasta or not os.path.isdir(pasta):
        return jsonify({"error": "Pasta não encontrada"}), 400

    result = legalmail_criar_rascunho(pasta, tribunal, sistema, comarca,
                                       assunto=assunto, uf_tribunal=uf_tribunal,
                                       client_data=client_data)
    if "error" in result:
        return jsonify(result), 400
    return jsonify(result)


@app.route("/api/legalmail/rascunho-lote", methods=["POST"])
def legalmail_rascunho_lote():
    """Create draft petitions for multiple client folders."""
    from flask import Response, stream_with_context
    data = request.get_json()
    pastas = data.get("pastas", [])
    tribunal = data.get("tribunal", "TRF-4")
    sistema = data.get("sistema", "eproc_jfpr")
    comarca = data.get("comarca", "")
    assunto = data.get("assunto", "") or None

    uf_tribunal = data.get("uf", "")

    def generate():
        results = []
        for i, pasta in enumerate(pastas):
            pasta = pasta.strip()
            cliente = os.path.basename(pasta).split("_")[0].strip()
            yield f"data: {json.dumps({'type': 'progress', 'index': i, 'total': len(pastas), 'cliente': cliente, 'step': 'enviando'})}\n\n"

            if not os.path.isdir(pasta):
                yield f"data: {json.dumps({'type': 'error', 'index': i, 'cliente': cliente, 'message': 'Pasta nao encontrada'})}\n\n"
                continue

            try:
                result = legalmail_criar_rascunho(pasta, tribunal, sistema, comarca,
                                                   assunto=assunto, uf_tribunal=uf_tribunal)
                if "error" in result:
                    yield f"data: {json.dumps({'type': 'error', 'index': i, 'cliente': cliente, 'message': result['error']})}\n\n"
                else:
                    result["cliente"] = cliente
                    results.append(result)
                    yield f"data: {json.dumps({'type': 'done', 'index': i, 'total': len(pastas), 'cliente': cliente, 'idpeticoes': result['idpeticoes'], 'anexos': result['uploaded_attachments']})}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'type': 'error', 'index': i, 'cliente': cliente, 'message': str(e)})}\n\n"

        yield f"data: {json.dumps({'type': 'complete', 'results': results})}\n\n"

    return Response(stream_with_context(generate()), mimetype='text/event-stream')


###############################################################################
# MONITORAMENTO DE PROCESSOS E ANÁLISE DE INTIMAÇÕES
###############################################################################

import time
import threading
import datetime as _dt

# Persistent storage - PostgreSQL on Railway, JSON files locally
DATABASE_URL = os.environ.get("DATABASE_URL", "")
USE_DB = bool(DATABASE_URL)

# Local file fallback
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)
NOTIFICATIONS_FILE = os.path.join(DATA_DIR, "notifications.json")
PROCESSES_CACHE_FILE = os.path.join(DATA_DIR, "processes_cache.json")
MONITOR_STATE_FILE = os.path.join(DATA_DIR, "monitor_state.json")

# Initialize PostgreSQL if available
if USE_DB:
    import psycopg2
    import psycopg2.extras
    def _get_db():
        return psycopg2.connect(DATABASE_URL)
    # Create tables on startup
    try:
        conn = _get_db()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                value JSONB NOT NULL DEFAULT '[]'::jsonb,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("[DB] PostgreSQL conectado e tabelas criadas")
    except Exception as e:
        print(f"[DB] Erro ao conectar PostgreSQL: {e}")
        USE_DB = False

# Monitor settings
MONITOR_INTERVAL_MINUTES = 1440  # Check once per day (24h = 1440 min)
_monitor_thread = None
_monitor_running = False


def _db_load(key, default=None):
    """Load JSON data from PostgreSQL."""
    if default is None:
        default = []
    try:
        conn = _get_db()
        cur = conn.cursor()
        cur.execute("SELECT value FROM kv_store WHERE key = %s", (key,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row[0] if row else default
    except Exception as e:
        print(f"[DB] Erro ao ler {key}: {e}")
        return default


def _db_save(key, data):
    """Save JSON data to PostgreSQL."""
    try:
        conn = _get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO kv_store (key, value, updated_at) VALUES (%s, %s, NOW())
            ON CONFLICT (key) DO UPDATE SET value = %s, updated_at = NOW()
        """, (key, json.dumps(data, ensure_ascii=False), json.dumps(data, ensure_ascii=False)))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[DB] Erro ao salvar {key}: {e}")


def _load_json_file(filepath, default=None):
    if default is None:
        default = []
    # Use DB if available, with filepath as key
    if USE_DB:
        key = os.path.basename(filepath).replace(".json", "")
        return _db_load(key, default)
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return default


def _save_json_file(filepath, data):
    if USE_DB:
        key = os.path.basename(filepath).replace(".json", "")
        _db_save(key, data)
        return
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _load_monitor_state():
    return _load_json_file(MONITOR_STATE_FILE, {
        "last_check": None,
        "known_movements": {},  # idprocesso -> set of idmovimentacoes
        "auto_analyze": True,
    })


def _save_monitor_state(state):
    _save_json_file(MONITOR_STATE_FILE, state)


def monitor_fetch_all_processes():
    """Fetch all processes from workspace. Returns list."""
    import requests
    all_procs = []
    offset = 0
    while True:
        try:
            r = legalmail_request("get", f"/process/all?offset={offset}&limit=50")
            if r.status_code == 429:
                time.sleep(60)
                r = legalmail_request("get", f"/process/all?offset={offset}&limit=50")
            if r.status_code != 200:
                break
            data = r.json()
            if not isinstance(data, list) or len(data) == 0:
                break
            all_procs.extend(data)
            if len(data) < 50:
                break
            offset += 50
            time.sleep(2)  # Be gentle with rate limits
        except Exception as e:
            print(f"  [MONITOR] Erro ao buscar processos offset={offset}: {e}")
            break
    return all_procs


def monitor_fetch_autos(idprocesso):
    """Fetch autos/movements for a process. Returns list of movements."""
    try:
        r = legalmail_request("get", f"/process/autos?idprocesso={idprocesso}")
        if r.status_code == 429:
            time.sleep(60)
            r = legalmail_request("get", f"/process/autos?idprocesso={idprocesso}")
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                return data
    except Exception as e:
        print(f"  [MONITOR] Erro ao buscar autos processo {idprocesso}: {e}")
    return []


def monitor_fetch_movement_text(idmovimentacoes):
    """Download movement document and extract text."""
    try:
        r = legalmail_request("get", f"/process/movement/url?idmovimentacoes={idmovimentacoes}")
        if r.status_code == 429:
            time.sleep(60)
            r = legalmail_request("get", f"/process/movement/url?idmovimentacoes={idmovimentacoes}")
        if r.status_code != 200:
            return None
        data = r.json()
        s3_url = data.get("s3_url", "")
        if not s3_url:
            return None

        # Download PDF and extract text
        import requests as req_lib
        pdf_resp = req_lib.get(s3_url, timeout=30)
        if pdf_resp.status_code != 200:
            return None

        pdf_doc = fitz.open(stream=pdf_resp.content, filetype="pdf")
        text = ""
        for page in pdf_doc:
            text += page.get_text()
        pdf_doc.close()
        return text.strip() if text.strip() else None
    except Exception as e:
        print(f"  [MONITOR] Erro ao baixar movimentação {idmovimentacoes}: {e}")
    return None


def monitor_analyze_movement(numero_processo, tribunal, titulo, texto):
    """Use Claude to analyze a movement/intimation."""
    try:
        client_ai = anthropic.Anthropic()
        prompt = f"""Você é um assistente jurídico. Analise esta movimentação processual:

Processo: {numero_processo} | Tribunal: {tribunal}
Título: {titulo}

TEXTO:
{texto[:6000]}

Responda APENAS com JSON válido:
{{"resumo": "resumo claro e objetivo (2-3 frases)", "tipo_movimentacao": "intimação|despacho|sentença|decisão|citação|audiência|perícia|expediente|outro", "prazo_dias": 0, "data_prazo": null, "urgencia": "alta|media|baixa", "acao_necessaria": "ação específica do advogado", "tipo_peticao_sugerida": "contestação|recurso|apelação|agravo|manifestação|cumprimento|embargos|impugnação|outro|nenhuma", "resultado_merito": null, "observacoes": "pontos relevantes"}}"""

        response = client_ai.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}]
        )
        response_text = response.content[0].text.strip()
        if "```json" in response_text:
            response_text = response_text.split("```json")[1].split("```")[0].strip()
        elif "```" in response_text:
            response_text = response_text.split("```")[1].split("```")[0].strip()
        return json.loads(response_text)
    except Exception as e:
        print(f"  [MONITOR] Erro na análise IA: {e}")
    return None


def monitor_check_updates():
    """Main monitoring function: check all processes for new movements.

    How it works:
    - Fetches the full process list from LegalMail (/process/all)
    - Compares each process's `last_import` date with our stored date
    - Only fetches autos for processes that have been updated since last check
    - On FIRST RUN: stores all current movement IDs as "known" (no flood of old notifications)
    - On subsequent runs: only creates notifications for truly NEW movements
    - Auto-analyzes new movements with Claude AI
    """
    print(f"\n[MONITOR] Verificando atualizacoes... {_dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    state = _load_monitor_state()
    notifications = _load_notifications()
    known = state.get("known_movements", {})
    last_imports = state.get("last_imports", {})
    is_first_run = state.get("first_run_done") is not True
    new_count = 0

    # Step 1: Fetch all processes
    processes = monitor_fetch_all_processes()
    if not processes:
        print("  [MONITOR] Nenhum processo encontrado")
        return 0

    # Save processes cache
    _save_json_file(PROCESSES_CACHE_FILE, processes)

    # Only active processes (with inbox_atual = being monitored by LegalMail)
    active_processes = [p for p in processes if p.get("inbox_atual")]
    print(f"  [MONITOR] {len(processes)} processos total, {len(active_processes)} ativos")

    if is_first_run:
        print(f"  [MONITOR] PRIMEIRA EXECUCAO - registrando movimentacoes existentes (sem gerar alertas)")

    # Step 2: Check which processes have new updates
    checked = 0
    for proc in active_processes:
        idprocesso = str(proc.get("idprocessos", ""))
        numero = proc.get("numero_processo", "?")
        tribunal = proc.get("tribunal", "?")
        proc_last_import = proc.get("last_import") or ""

        if not idprocesso:
            continue

        # Skip if last_import hasn't changed since our last check
        stored_import = last_imports.get(idprocesso, "")
        if not is_first_run and stored_import and proc_last_import == stored_import:
            continue  # No new data for this process

        # Update stored last_import
        last_imports[idprocesso] = proc_last_import

        time.sleep(2)  # Rate limit safety
        autos = monitor_fetch_autos(idprocesso)
        if not autos:
            continue

        # Get current movement IDs
        current_ids = [str(m.get("idmovimentacoes", "")) for m in autos]
        known_ids = set(known.get(idprocesso, []))

        if is_first_run:
            # First run: just store all IDs as known, don't create notifications
            known[idprocesso] = current_ids
            checked += 1
        else:
            # Subsequent runs: find truly new movements
            new_movements = [m for m in autos if str(m.get("idmovimentacoes", "")) not in known_ids]

            # Filter: only movements from yesterday or today
            yesterday = (_dt.datetime.now() - _dt.timedelta(days=1)).strftime("%Y-%m-%d")
            today = _dt.datetime.now().strftime("%Y-%m-%d")
            new_movements = [m for m in new_movements
                             if (m.get("data_movimentacao", "") or "")[:10] >= yesterday]

            # Sort by date (most recent last)
            new_movements.sort(key=lambda m: m.get("data_movimentacao", "") or "")

            if new_movements:
                print(f"  [MONITOR] {len(new_movements)} nova(s) movimentacao(oes) em {numero}")

                for mov in new_movements:
                    mov_id = str(mov.get("idmovimentacoes", ""))
                    titulo = mov.get("titulo", "Movimentacao")
                    data_mov = mov.get("data_movimentacao", "")

                    # Create notification
                    notif = {
                        "type": "intimacao",
                        "source": "monitor_auto",
                        "timestamp": _dt.datetime.now().isoformat(),
                        "numero_processo": numero,
                        "tribunal": tribunal,
                        "sistema": proc.get("sistema_tribunal", ""),
                        "polo_ativo": proc.get("poloativo_nome", ""),
                        "polo_passivo": proc.get("polopassivo_nome", ""),
                        "classe": proc.get("nome_classe", ""),
                        "idprocesso": idprocesso,
                        "idmovimentacoes": mov_id,
                        "titulo_movimentacao": titulo,
                        "data_movimentacao": data_mov,
                        "documentos": [{
                            "tipo": "movement",
                            "title": titulo,
                            "movement_date": data_mov,
                            "idmovimentacoes": mov_id,
                        }],
                        "analyzed": False,
                        "analysis": None,
                    }

                    # Fetch movement text (for manual analysis later) but do NOT auto-analyze
                    time.sleep(2)
                    texto = monitor_fetch_movement_text(mov_id)
                    if texto:
                        notif["texto_movimentacao"] = texto[:8000]
                    print(f"    -> {titulo[:80]}")

                    notifications.append(notif)
                    new_count += 1

                # Update known movements
                known[idprocesso] = current_ids

            checked += 1

        # Don't check too many in one run to stay within rate limits
        if checked >= 30:
            print(f"  [MONITOR] Limite de verificacoes atingido, continuando no proximo ciclo")
            break

    # Save state
    state["known_movements"] = known
    state["last_imports"] = last_imports
    state["last_check"] = _dt.datetime.now().isoformat()
    state["processes_total"] = len(processes)
    state["processes_active"] = len(active_processes)
    state["first_run_done"] = True
    _save_monitor_state(state)
    _save_notifications(notifications)

    if is_first_run:
        print(f"  [MONITOR] Primeira execucao concluida: {checked} processos registrados")
    else:
        print(f"  [MONITOR] Concluido: {new_count} nova(s) notificacao(oes), {checked} processos verificados")
    return new_count


def _monitor_loop():
    """Background loop that runs monitoring periodically."""
    global _monitor_running
    # Wait a bit for app to start
    time.sleep(10)
    print(f"\n[MONITOR] Monitor automático iniciado (intervalo: {MONITOR_INTERVAL_MINUTES} min)")

    while _monitor_running:
        try:
            monitor_check_updates()
        except Exception as e:
            print(f"  [MONITOR] Erro no ciclo: {e}")

        # Sleep in small increments so we can stop cleanly
        for _ in range(MONITOR_INTERVAL_MINUTES * 60):
            if not _monitor_running:
                break
            time.sleep(1)

    print("[MONITOR] Monitor automático parado")


def start_monitor():
    """Start the background monitoring thread."""
    global _monitor_thread, _monitor_running
    if _monitor_running:
        return False
    _monitor_running = True
    _monitor_thread = threading.Thread(target=_monitor_loop, daemon=True)
    _monitor_thread.start()
    return True


def stop_monitor():
    """Stop the background monitoring thread."""
    global _monitor_running
    _monitor_running = False


@app.route("/api/legalmail/monitor/status", methods=["GET"])
def legalmail_monitor_status():
    """Get monitor status and stats."""
    state = _load_monitor_state()
    notifications = _load_notifications()
    pending = [n for n in notifications if not n.get("analyzed")]
    urgent = [n for n in notifications if n.get("analysis", {}).get("urgencia") == "alta"]
    return jsonify({
        "running": _monitor_running,
        "interval_minutes": MONITOR_INTERVAL_MINUTES,
        "last_check": state.get("last_check"),
        "processes_total": state.get("processes_total", 0),
        "processes_active": state.get("processes_active", 0),
        "notifications_total": len(notifications),
        "notifications_pending": len(pending),
        "notifications_urgent": len(urgent),
        "auto_analyze": state.get("auto_analyze", True),
    })


@app.route("/api/legalmail/monitor/start", methods=["POST"])
def legalmail_monitor_start():
    """Start background monitoring."""
    started = start_monitor()
    return jsonify({"status": "ok" if started else "already_running", "running": _monitor_running})


@app.route("/api/legalmail/monitor/stop", methods=["POST"])
def legalmail_monitor_stop():
    """Stop background monitoring."""
    stop_monitor()
    return jsonify({"status": "ok", "running": False})


@app.route("/api/legalmail/monitor/check-now", methods=["POST"])
def legalmail_monitor_check_now():
    """Run a manual check immediately (in a thread to not block)."""
    def _run():
        try:
            monitor_check_updates()
        except Exception as e:
            print(f"  [MONITOR] Erro: {e}")
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return jsonify({"status": "ok", "message": "Verificação iniciada em background"})


@app.route("/api/legalmail/monitor/config", methods=["POST"])
def legalmail_monitor_config():
    """Update monitor configuration."""
    global MONITOR_INTERVAL_MINUTES
    data = request.get_json()
    state = _load_monitor_state()
    if "interval_minutes" in data:
        MONITOR_INTERVAL_MINUTES = max(5, int(data["interval_minutes"]))
    if "auto_analyze" in data:
        state["auto_analyze"] = bool(data["auto_analyze"])
    _save_monitor_state(state)
    return jsonify({"status": "ok", "interval_minutes": MONITOR_INTERVAL_MINUTES, "auto_analyze": state["auto_analyze"]})


@app.route("/api/legalmail/processo/autos", methods=["GET"])
def legalmail_processo_autos():
    """Get movements/autos for a specific process."""
    idprocesso = request.args.get("idprocesso", "")
    if not idprocesso:
        return jsonify({"error": "idprocesso obrigatório"}), 400
    resp = legalmail_request("get", f"/process/autos?idprocesso={idprocesso}")
    if resp.status_code == 200:
        return jsonify(resp.json())
    return jsonify({"error": resp.text[:300]}), resp.status_code


def _load_notifications():
    return _load_json_file(NOTIFICATIONS_FILE, [])


def _save_notifications(notifications):
    _save_json_file(NOTIFICATIONS_FILE, notifications)


@app.route("/api/legalmail/processos", methods=["GET"])
def legalmail_listar_processos():
    """List all processes in the workspace."""
    offset = request.args.get("offset", 0, type=int)
    limit = request.args.get("limit", 50, type=int)
    resp = legalmail_request("get", f"/process/all?offset={offset}&limit={limit}")
    if resp.status_code == 200:
        return jsonify(resp.json())
    return jsonify({"error": resp.text[:300]}), resp.status_code


@app.route("/api/legalmail/processo/detalhe", methods=["GET"])
def legalmail_detalhe_processo():
    """Get details of a specific process."""
    numero = request.args.get("numero_processo", "")
    idprocesso = request.args.get("idprocesso", "")
    params = ""
    if numero:
        params = f"numero_processo={numero}"
    elif idprocesso:
        params = f"idprocesso={idprocesso}"
    else:
        return jsonify({"error": "Informe numero_processo ou idprocesso"}), 400
    resp = legalmail_request("get", f"/process/detail?{params}")
    if resp.status_code == 200:
        return jsonify(resp.json())
    return jsonify({"error": resp.text[:300]}), resp.status_code


@app.route("/api/legalmail/processo/movimentacao-url", methods=["GET"])
def legalmail_movimentacao_url():
    """Get S3 URL for a movement document."""
    idmovimentacoes = request.args.get("idmovimentacoes", "")
    if not idmovimentacoes:
        return jsonify({"error": "idmovimentacoes obrigatório"}), 400
    resp = legalmail_request("get", f"/process/movement/url?idmovimentacoes={idmovimentacoes}")
    if resp.status_code == 200:
        return jsonify(resp.json())
    return jsonify({"error": resp.text[:300]}), resp.status_code


@app.route("/api/legalmail/importar", methods=["POST"])
def legalmail_importar_processo():
    """Import processes for monitoring.

    Body: { "pedidos": [{"numero": "...", "tribunal": "TRF-3", "sistema": "pje", "classe_id": "143"}] }
    """
    data = request.get_json()
    pedidos = data.get("pedidos", [])
    if not pedidos:
        return jsonify({"error": "Nenhum pedido informado"}), 400

    # Add certificado_id to each pedido if not present
    for p in pedidos:
        if "certificado_id" not in p:
            p["certificado_id"] = LEGALMAIL_CERT_ID

    resp = legalmail_request("post", "/imports/request", json={"pedidos": pedidos})
    if resp.status_code == 200:
        return jsonify(resp.json())
    return jsonify({"error": resp.text[:300]}), resp.status_code


@app.route("/api/legalmail/importar/status", methods=["GET"])
def legalmail_importar_status():
    """Check import status by hash or process number."""
    hash_val = request.args.get("hash", "")
    numero = request.args.get("numero", "")
    params = ""
    if hash_val:
        params = f"hash={hash_val}"
    elif numero:
        params = f"numero={numero}"
    else:
        return jsonify({"error": "Informe hash ou numero"}), 400
    resp = legalmail_request("get", f"/imports/status?{params}")
    if resp.status_code == 200:
        return jsonify(resp.json())
    return jsonify({"error": resp.text[:300]}), resp.status_code


@app.route("/api/legalmail/importar/classes", methods=["GET"])
def legalmail_importar_classes():
    """Search import classes by term."""
    term = request.args.get("term", "")
    resp = legalmail_request("get", f"/imports/classes?term={term}")
    if resp.status_code == 200:
        return jsonify(resp.json())
    return jsonify({"error": resp.text[:300]}), resp.status_code


@app.route("/api/legalmail/importar/sistemas", methods=["GET"])
def legalmail_importar_sistemas():
    """Get allowed systems for a process number."""
    numero = request.args.get("numero", "")
    if not numero:
        return jsonify({"error": "numero obrigatório"}), 400
    resp = legalmail_request("get", f"/imports/allowed-systems?numero={numero}")
    if resp.status_code == 200:
        return jsonify(resp.json())
    return jsonify({"error": resp.text[:300]}), resp.status_code


@app.route("/api/legalmail/webhook/configurar", methods=["POST"])
def legalmail_configurar_webhook():
    """Register webhook endpoint for notifications.

    Body: { "endpoint": "https://your-server.com/api/legalmail/webhook", "key_endpoint": "optional-secret" }
    """
    data = request.get_json()
    endpoint_url = data.get("endpoint", "")
    key_endpoint = data.get("key_endpoint", "")
    if not endpoint_url:
        return jsonify({"error": "endpoint obrigatório"}), 400

    params = f"endpoint={endpoint_url}"
    if key_endpoint:
        params += f"&key_endpoint={key_endpoint}"
    resp = legalmail_request("post", f"/workspace/notifications/endpoint?{params}")
    if resp.status_code == 200:
        return jsonify(resp.json())
    return jsonify({"error": resp.text[:300]}), resp.status_code


@app.route("/api/legalmail/webhook", methods=["POST"])
def legalmail_webhook_receiver():
    """Receive webhook notifications from LegalMail.

    LegalMail sends new intimações/movimentações here automatically.
    We store them and can trigger analysis.
    """
    payload = request.get_json(force=True, silent=True)
    if not payload:
        return jsonify({"status": "ignored", "reason": "empty payload"}), 200

    notifications = _load_notifications()

    params = payload.get("params", [])
    import datetime
    timestamp = datetime.datetime.now().isoformat()

    for item in params:
        # Check if it's a protocol event
        if item.get("event") == "protocolo_finalizado":
            notifications.append({
                "type": "protocolo_finalizado",
                "timestamp": timestamp,
                "data": item,
                "analyzed": False,
            })
            continue

        # It's an intimation/movement notification
        notification = {
            "type": "intimacao",
            "timestamp": timestamp,
            "numero_processo": item.get("numero_processo", ""),
            "tribunal": item.get("tribunal", ""),
            "sistema": item.get("sistema_tribunal", ""),
            "polo_ativo": item.get("polo_ativo", ""),
            "polo_passivo": item.get("polo_passivo", ""),
            "classe": item.get("classe_processo", ""),
            "inbox_id": item.get("inbox_id", ""),
            "inbox_uri": item.get("inbox_uri", ""),
            "documentos": item.get("documento", []),
            "analyzed": False,
            "analysis": None,
        }
        notifications.append(notification)

    _save_notifications(notifications)
    print(f"  [WEBHOOK] Recebidas {len(params)} notificações")
    return jsonify({"status": "ok", "received": len(params)}), 200


@app.route("/api/legalmail/notificacoes", methods=["GET"])
def legalmail_listar_notificacoes():
    """List received webhook notifications."""
    notifications = _load_notifications()
    only_pending = request.args.get("pending", "false").lower() == "true"
    if only_pending:
        notifications = [n for n in notifications if not n.get("analyzed")]
    # Filter: only last 5 days
    cutoff = (_dt.datetime.now() - _dt.timedelta(days=5)).strftime("%Y-%m-%d")
    notifications = [n for n in notifications
                     if (n.get("data_movimentacao") or n.get("timestamp") or "")[:10] >= cutoff]
    # Sort by date (most recent first)
    notifications.sort(
        key=lambda n: n.get("data_movimentacao") or n.get("timestamp") or "",
        reverse=True
    )
    return jsonify(notifications)


@app.route("/api/legalmail/notificacoes/importar", methods=["POST"])
def legalmail_importar_notificacoes():
    """Import notifications from JSON array (replaces existing)."""
    data = request.get_json()
    if not isinstance(data, list):
        return jsonify({"error": "Esperado um array JSON"}), 400
    _save_notifications(data)
    return jsonify({"status": "ok", "imported": len(data)})


@app.route("/api/legalmail/notificacoes/limpar", methods=["POST"])
def legalmail_limpar_notificacoes():
    """Remove notifications older than 5 days from storage."""
    notifications = _load_notifications()
    total_before = len(notifications)
    cutoff = (_dt.datetime.now() - _dt.timedelta(days=5)).strftime("%Y-%m-%d")
    notifications = [n for n in notifications
                     if (n.get("data_movimentacao") or n.get("timestamp") or "")[:10] >= cutoff]
    _save_notifications(notifications)
    removed = total_before - len(notifications)
    return jsonify({"status": "ok", "removed": removed, "remaining": len(notifications)})


@app.route("/api/legalmail/notificacao/analisar", methods=["POST"])
def legalmail_analisar_intimacao():
    """Analyze an intimation using Claude AI.

    Body: { "index": 0 }  (index in notifications list - legacy)
    or: { "timestamp": "..." }  (find by timestamp - preferred)
    or: { "texto": "...", "numero_processo": "..." }  (direct text)
    """
    data = request.get_json()

    # Get text to analyze
    texto_intimacao = ""
    numero_processo = ""
    notif_index = data.get("index")
    notif_timestamp = data.get("timestamp")

    # Find notification by timestamp (preferred) or index (legacy)
    notif = None
    notifications = None
    if notif_timestamp:
        notifications = _load_notifications()
        for i, n in enumerate(notifications):
            if n.get("timestamp") == notif_timestamp:
                notif = n
                notif_index = i
                break
        if notif is None:
            return jsonify({"error": "Notificação não encontrada"}), 400
    elif notif_index is not None:
        notifications = _load_notifications()
        if notif_index < 0 or notif_index >= len(notifications):
            return jsonify({"error": "Índice inválido"}), 400
        notif = notifications[notif_index]

    if notif:
        numero_processo = notif.get("numero_processo", "")
        # Check if monitor already extracted text
        if notif.get("texto_movimentacao"):
            texto_intimacao = notif["texto_movimentacao"]

        # Collect all text from documents
        docs = notif.get("documentos", [])
        for doc in docs:
            if doc.get("tipo") == "movement" and not texto_intimacao:
                # Monitor movement - try to fetch text
                mov_id = doc.get("idmovimentacoes")
                if mov_id:
                    texto = monitor_fetch_movement_text(mov_id)
                    if texto:
                        texto_intimacao += f"\n--- {doc.get('title', 'Movimentação')} ({doc.get('movement_date', '')}) ---\n"
                        texto_intimacao += texto
            elif doc.get("tipo") == "text":
                texto_intimacao += f"\n--- {doc.get('title', 'Movimentação')} ({doc.get('movement_date', '')}) ---\n"
                texto_intimacao += doc.get("text", "")
            elif doc.get("tipo") == "pdf":
                # Download and extract text from PDF
                pdf_url = doc.get("link", "")
                if pdf_url:
                    try:
                        import requests as req_lib
                        pdf_resp = req_lib.get(pdf_url, timeout=30)
                        if pdf_resp.status_code == 200:
                            pdf_doc = fitz.open(stream=pdf_resp.content, filetype="pdf")
                            pdf_text = ""
                            for page in pdf_doc:
                                pdf_text += page.get_text()
                            pdf_doc.close()
                            texto_intimacao += f"\n--- {doc.get('title', 'Documento PDF')} ({doc.get('movement_date', '')}) ---\n"
                            texto_intimacao += pdf_text
                    except Exception as e:
                        texto_intimacao += f"\n[Erro ao baixar PDF: {e}]\n"
    else:
        texto_intimacao = data.get("texto", "")
        numero_processo = data.get("numero_processo", "")

    if not texto_intimacao:
        return jsonify({"error": "Nenhum texto para analisar"}), 400

    # Use Claude to analyze
    client = anthropic.Anthropic()
    analysis_prompt = f"""Você é um assistente jurídico experiente.

Analise a movimentação processual abaixo com atenção a:
- Há PRAZO para o advogado? (intimação, contestação, recurso, cumprimento, etc.)
- Calcule a data limite do prazo considerando dias úteis quando aplicável
- Urgência real: sentença/decisão de mérito = alta, intimação com prazo = alta, mero expediente = baixa
- Indique de forma CLARA e DIRETA o que o advogado precisa fazer
- Se for sentença: indique se foi procedente, improcedente ou parcialmente procedente

Processo: {numero_processo}

TEXTO:
{texto_intimacao[:8000]}

Responda APENAS com JSON válido:
{{
    "resumo": "Resumo claro e objetivo do que aconteceu (2-3 frases)",
    "tipo_movimentacao": "intimação|despacho|sentença|decisão|citação|audiência|perícia|expediente|outro",
    "prazo_dias": número de dias do prazo (0 se não há),
    "data_prazo": "AAAA-MM-DD" ou null,
    "urgencia": "alta|media|baixa",
    "acao_necessaria": "Ação específica que o advogado deve tomar (ex: 'Interpor recurso em 15 dias úteis' ou 'Nenhuma ação necessária, apenas ciência')",
    "tipo_peticao_sugerida": "contestação|recurso|apelação|agravo|manifestação|cumprimento|embargos|impugnação|outro|nenhuma",
    "resultado_merito": "procedente|improcedente|parcialmente_procedente|null (se não for sentença/decisão de mérito)",
    "observacoes": "Pontos relevantes para o advogado"
}}"""

    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1500,
            messages=[{"role": "user", "content": analysis_prompt}]
        )
        response_text = response.content[0].text.strip()

        # Parse JSON from response (handle markdown code blocks)
        if "```json" in response_text:
            response_text = response_text.split("```json")[1].split("```")[0].strip()
        elif "```" in response_text:
            response_text = response_text.split("```")[1].split("```")[0].strip()

        analysis = json.loads(response_text)

        # Save analysis back to notification if index provided
        if notif_index is not None:
            notifications = _load_notifications()
            if notif_index < len(notifications):
                notifications[notif_index]["analyzed"] = True
                notifications[notif_index]["analysis"] = analysis
                _save_notifications(notifications)

        return jsonify({"status": "ok", "analysis": analysis})

    except json.JSONDecodeError as e:
        return jsonify({"error": f"Erro ao parsear resposta da IA: {e}", "raw": response_text}), 500
    except Exception as e:
        return jsonify({"error": f"Erro na análise: {e}"}), 500


@app.route("/api/legalmail/peticao-intermediaria", methods=["POST"])
def legalmail_criar_peticao_intermediaria():
    """Create an intermediate petition for an existing process.

    Body: {
        "idprocesso": 123,
        "texto_peticao": "optional - text to generate petition",
        "pdf_path": "optional - path to existing PDF",
        "tipo_peticao": "manifestação|contestação|recurso|...",
        "anexos": ["path1.pdf", "path2.pdf"]
    }
    """
    data = request.get_json()
    idprocesso = data.get("idprocesso")
    texto_peticao = data.get("texto_peticao", "")
    pdf_path = data.get("pdf_path", "")
    tipo_peticao = data.get("tipo_peticao", "Manifestação")
    anexos = data.get("anexos", [])

    if not idprocesso:
        return jsonify({"error": "idprocesso obrigatório"}), 400

    import time

    # Step 1: Create intermediate petition
    payload = {
        "fk_processo": int(idprocesso),
        "fk_certificado": LEGALMAIL_CERT_ID,
        "tutela_antecipada": 0,
        "custas_recolhidas": 0,
    }
    resp = legalmail_request("post", "/petition/intermediate", json=payload)
    if resp.status_code == 429:
        time.sleep(60)
        resp = legalmail_request("post", "/petition/intermediate", json=payload)

    if resp.status_code != 200:
        return jsonify({"error": f"Erro ao criar petição: {resp.status_code} {resp.text[:200]}"}), 400

    try:
        resp_data = resp.json()
    except Exception:
        return jsonify({"error": f"Resposta inválida: {resp.text[:200]}"}), 400

    idpeticoes = resp_data.get("idpeticoes")
    if not idpeticoes:
        return jsonify({"error": f"Sem idpeticoes na resposta: {resp_data}"}), 400

    result = {
        "idpeticoes": idpeticoes,
        "idprocesso": idprocesso,
    }

    # Step 2: If text provided, generate PDF with Claude
    if texto_peticao and not pdf_path:
        try:
            client_ai = anthropic.Anthropic()
            gen_resp = client_ai.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=4000,
                messages=[{"role": "user", "content": f"Gere o texto completo da seguinte petição intermediária ({tipo_peticao}) para protocolar nos autos. O texto deve ser formal, técnico e pronto para protocolar:\n\n{texto_peticao}"}]
            )
            peticao_texto = gen_resp.content[0].text

            # Convert to PDF using PyMuPDF
            pdf_path = os.path.join(OUTPUT_DIR, f"peticao_intermediaria_{idpeticoes}.pdf")
            doc = fitz.open()
            page = doc.new_page()
            text_rect = fitz.Rect(72, 72, page.rect.width - 72, page.rect.height - 72)
            # Split text into chunks that fit on pages
            remaining = peticao_texto
            while remaining:
                rc = page.insert_textbox(text_rect, remaining, fontsize=11, fontname="helv")
                if rc >= 0:
                    break
                # Text overflowed - need more pages
                overflow_idx = len(remaining) - int(abs(rc))
                remaining = remaining[overflow_idx:]
                if remaining.strip():
                    page = doc.new_page()
                else:
                    break
            doc.save(pdf_path)
            doc.close()
            result["generated_pdf"] = pdf_path
        except Exception as e:
            result["warn_generation"] = f"Erro ao gerar PDF: {e}"

    # Step 3: Upload petition PDF
    if pdf_path and os.path.exists(pdf_path):
        time.sleep(2)
        with open(pdf_path, 'rb') as pf:
            resp = legalmail_request("post",
                f"/petition/file?idpeticoes={idpeticoes}&idprocessos={idprocesso}",
                files={"file": (os.path.basename(pdf_path), pf, "application/pdf")})
        if resp.status_code == 429:
            time.sleep(60)
            with open(pdf_path, 'rb') as pf:
                resp = legalmail_request("post",
                    f"/petition/file?idpeticoes={idpeticoes}&idprocessos={idprocesso}",
                    files={"file": (os.path.basename(pdf_path), pf, "application/pdf")})
        result["petition_upload"] = resp.status_code

    # Step 4: Upload attachments
    uploaded = 0
    for anexo_path in anexos:
        if os.path.exists(anexo_path):
            time.sleep(1)
            try:
                with open(anexo_path, 'rb') as af:
                    resp = legalmail_request("post",
                        f"/petition/attachments?idpeticoes={idpeticoes}&fk_documentos_tipos=1",
                        files={"file": (os.path.basename(anexo_path), af, "application/pdf")})
                if resp.status_code == 200:
                    uploaded += 1
                elif resp.status_code == 429:
                    time.sleep(60)
                    with open(anexo_path, 'rb') as af:
                        resp = legalmail_request("post",
                            f"/petition/attachments?idpeticoes={idpeticoes}&fk_documentos_tipos=1",
                            files={"file": (os.path.basename(anexo_path), af, "application/pdf")})
                    if resp.status_code == 200:
                        uploaded += 1
            except Exception as e:
                print(f"  [WARN] Erro anexo {anexo_path}: {e}")

    result["uploaded_attachments"] = uploaded

    # Step 5: Get available peças for send
    time.sleep(1)
    resp_types = legalmail_request("get", f"/petition/types?idpeticoes={idpeticoes}")
    if resp_types.status_code == 200:
        try:
            pecas = resp_types.json().get("pecas", [])
            result["pecas_disponiveis"] = pecas
        except Exception:
            pass

    result["status"] = "ok"
    result["url"] = f"https://app.legalmail.com.br/petitions/{idpeticoes}"
    return jsonify(result)


@app.route("/api/legalmail/peticao-intermediaria/protocolar", methods=["POST"])
def legalmail_protocolar_intermediaria():
    """Send/protocol an intermediate petition.

    Body: { "idpeticoes": 123, "idprocessos": 456, "fk_peca": 1, "data_protocolo": "2026-03-10" }
    """
    data = request.get_json()
    idpeticoes = data.get("idpeticoes")
    idprocessos = data.get("idprocessos")
    fk_peca = data.get("fk_peca")
    data_protocolo = data.get("data_protocolo", "")

    if not all([idpeticoes, idprocessos, fk_peca]):
        return jsonify({"error": "idpeticoes, idprocessos e fk_peca são obrigatórios"}), 400

    params = f"idpeticoes={idpeticoes}&idprocessos={idprocessos}&fk_peca={fk_peca}"
    if data_protocolo:
        params += f"&data_protocolo={data_protocolo}"

    resp = legalmail_request("post", f"/petition/intermediate/send?{params}")
    if resp.status_code == 200:
        return jsonify(resp.json())
    return jsonify({"error": resp.text[:300]}), resp.status_code


@app.route("/api/legalmail/analisar-todos", methods=["POST"])
def legalmail_analisar_todos_pendentes():
    """Analyze all pending (unanalyzed) notifications at once."""
    notifications = _load_notifications()
    pending = [(i, n) for i, n in enumerate(notifications)
               if not n.get("analyzed") and n.get("type") == "intimacao"]

    if not pending:
        return jsonify({"status": "ok", "message": "Nenhuma notificação pendente", "analyzed": 0})

    results = []
    for idx, notif in pending:
        # Collect text
        texto = ""
        for doc in notif.get("documentos", []):
            if doc.get("tipo") == "text":
                texto += f"\n{doc.get('title', '')}: {doc.get('text', '')}"
            elif doc.get("tipo") == "pdf" and doc.get("link"):
                try:
                    import requests as req_lib
                    pdf_resp = req_lib.get(doc["link"], timeout=30)
                    if pdf_resp.status_code == 200:
                        pdf_doc = fitz.open(stream=pdf_resp.content, filetype="pdf")
                        for page in pdf_doc:
                            texto += page.get_text()
                        pdf_doc.close()
                except Exception:
                    pass

        if not texto.strip():
            continue

        # Analyze with Claude
        try:
            client_ai = anthropic.Anthropic()
            analysis_prompt = f"""Analise esta intimação processual e responda APENAS com JSON:

Processo: {notif.get('numero_processo', '')}
Tribunal: {notif.get('tribunal', '')}

TEXTO:
{texto[:6000]}

JSON formato:
{{"resumo": "...", "tipo_movimentacao": "...", "prazo_dias": 0, "data_prazo": null, "urgencia": "alta|media|baixa", "acao_necessaria": "...", "tipo_peticao_sugerida": "...", "observacoes": "..."}}"""

            response = client_ai.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=1500,
                messages=[{"role": "user", "content": analysis_prompt}]
            )
            response_text = response.content[0].text.strip()
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()

            analysis = json.loads(response_text)
            notifications[idx]["analyzed"] = True
            notifications[idx]["analysis"] = analysis
            results.append({"index": idx, "processo": notif.get("numero_processo"), "analysis": analysis})
        except Exception as e:
            results.append({"index": idx, "processo": notif.get("numero_processo"), "error": str(e)})

    _save_notifications(notifications)
    return jsonify({"status": "ok", "analyzed": len(results), "results": results})


@app.route("/api/legalmail/buscar-processo", methods=["GET"])
def legalmail_buscar_processo():
    """Busca processo por número ou nome da parte — usa cache local."""
    numero = request.args.get("numero", "").strip()
    nome = request.args.get("nome", "").strip()

    if not numero and not nome:
        return jsonify({"error": "Informe numero ou nome"}), 400

    # Carrega cache local do monitor (evita rate limit)
    cache_path = os.path.join(BASE_DIR, "processes_cache.json")
    if os.path.exists(cache_path):
        with open(cache_path, encoding="utf-8") as f:
            todos = json.load(f)
    else:
        # Sem cache, busca primeira página da API
        resp = legalmail_request("get", "/process/all?offset=0&limit=50")
        todos = resp.json() if resp.status_code == 200 and isinstance(resp.json(), list) else []

    if numero:
        encontrados = [p for p in todos if (p.get("numero_processo") or "") == numero]
    elif nome:
        nome_lower = nome.lower()
        encontrados = [
            p for p in todos
            if nome_lower in (p.get("poloativo_nome") or "").lower()
            or nome_lower in (p.get("polopassivo_nome") or "").lower()
        ]
    else:
        encontrados = []

    # Enriquece com movimentações da API (com fallback nas notificações cacheadas)
    notificacoes = _load_notifications()
    for p in encontrados:
        idprocesso = p.get("idprocessos")
        numero_proc = p.get("numero_processo", "")
        movs = []
        try:
            resp_autos = legalmail_request("get", f"/process/autos?idprocesso={idprocesso}")
            if resp_autos.status_code == 200:
                autos = resp_autos.json()
                if isinstance(autos, list):
                    movs = autos[:10]
        except Exception:
            pass
        # Fallback: usa notificações cacheadas do monitor
        if not movs:
            movs = [
                {"titulo": n.get("titulo_movimentacao", ""), "data_movimentacao": n.get("data_movimentacao", "")}
                for n in notificacoes
                if n.get("numero_processo") == numero_proc or str(n.get("idprocesso")) == str(idprocesso)
            ][:10]
        p["movimentacoes"] = movs

    return jsonify({"processos": encontrados})


@app.route("/api/legalmail/gerar-mensagem-cliente", methods=["POST"])
def legalmail_gerar_mensagem_cliente():
    """Gera mensagem simples de WhatsApp explicando o andamento do processo ao cliente."""
    data = request.get_json() or {}
    processo = data.get("processo", {})
    movimentacoes = data.get("movimentacoes", [])
    instrucao_extra = data.get("instrucao", "")

    numero = processo.get("numero_processo", "")
    polo_ativo = processo.get("poloativo_nome", "")
    tribunal = processo.get("tribunal", "")
    classe = processo.get("nome_classe") or processo.get("abreviatura_classe", "")
    juizo = processo.get("juizo", "")
    data_prazo = processo.get("data_prazo", "")

    movs_texto = ""
    for m in movimentacoes[:5]:
        movs_texto += f"- {m.get('data_movimentacao', '')}: {m.get('titulo', '')}\n"

    prompt = f"""Você é um advogado que precisa enviar uma mensagem simples pelo WhatsApp para o cliente explicando o andamento do processo dele.

Dados do processo:
- Número: {numero}
- Cliente (polo ativo): {polo_ativo}
- Tribunal: {tribunal}
- Classe: {classe}
- Juízo: {juizo}
- Prazo: {data_prazo or 'Nenhum prazo imediato'}

Últimas movimentações:
{movs_texto or 'Nenhuma movimentação recente.'}

{f'Instrução adicional: {instrucao_extra}' if instrucao_extra else ''}

Escreva uma mensagem curta, clara e em linguagem simples (sem juridiquês) para o cliente. Use o primeiro nome dele. Explique o que está acontecendo no processo e o que ele precisa saber. Se houver prazo, mencione. Não inclua saudações formais, apenas a mensagem direta. Não inclua títulos, cabeçalhos ou marcações como "# Mensagem para WhatsApp" ou similares. Nunca mencione valores financeiros do processo."""

    try:
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            return jsonify({"error": "ANTHROPIC_API_KEY não configurada"}), 500
        client_ai = anthropic.Anthropic(api_key=api_key, timeout=60.0)
        response = client_ai.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}]
        )
        mensagem = response.content[0].text.strip()
        return jsonify({"mensagem": mensagem})
    except Exception as e:
        return jsonify({"error": f"{type(e).__name__}: {str(e)}"}), 500


@app.route("/api/health")
def health_check():
    """Test API connectivity."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    result = {"api_key_set": bool(api_key), "api_key_prefix": api_key[:12] + "..." if api_key else "empty"}
    try:
        client_ai = anthropic.Anthropic(api_key=api_key, timeout=30.0)
        response = client_ai.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=5,
            messages=[{"role": "user", "content": "oi"}]
        )
        result["api_status"] = "ok"
        result["response"] = response.content[0].text[:50]
    except Exception as e:
        result["api_status"] = "error"
        result["error"] = f"{type(e).__name__}: {str(e)}"
    return jsonify(result)


@app.route("/monitoramento")
def monitoramento_page():
    """Page for process monitoring and intimation analysis."""
    return render_template("monitoramento.html")


# Inicialização — roda tanto com gunicorn quanto python app.py
def _startup():
    if LEGALMAIL_API_KEY:
        # Se não há cache ainda, popula em background antes de iniciar o monitor
        if not os.path.exists(PROCESSES_CACHE_FILE):
            def _pre_cache():
                print("[STARTUP] Cache vazio, buscando processos...")
                procs = monitor_fetch_all_processes()
                if procs:
                    _save_json_file(PROCESSES_CACHE_FILE, procs)
                    print(f"[STARTUP] Cache populado com {len(procs)} processos")
            threading.Thread(target=_pre_cache, daemon=True).start()
        start_monitor()
        print(f"Monitor automático ativado (verifica a cada {MONITOR_INTERVAL_MINUTES} min)")
    else:
        print("AVISO: LEGALMAIL_API_KEY não configurada - monitor desativado")

try:
    _startup()
except Exception as e:
    print(f"[STARTUP ERROR] {e}")

if __name__ == "__main__":
    print(f"Diretório da aplicação: {BASE_DIR}")
    print(f"Timbrado: {TIMBRADO_PATH}")
    print(f"Output: {OUTPUT_DIR}")
    print("\nIniciando servidor em http://localhost:5000")
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", debug=False, port=port)
