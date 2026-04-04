"""
PROCESSAMENTO COMPLETO EM LOTE v3 — BPC/LOAS
Fixes #19-25 from 24-25/03/2026 session.

Pipeline:
1. Valida CEP/CPF de todos os clientes ANTES de chamar API (fix #25)
2. Separa PJe e eProc em filas distintas (fix #24)
3. Processa PJe primeiro (simples), eProc depois (sequencia rigida)
4. Checkpoint por caso — pode retomar se crashar (fix #23)
5. Nao carrega Flask/scheduler/monitor (fix #21/#22)

Uso:
    python processar_lote_v2.py "H:\\Meu Drive\\JUDICIAIS\\NOVOS\\CLIENTE1" "CLIENTE2" ...
    python processar_lote_v2.py --pasta-base "H:\\Meu Drive\\JUDICIAIS\\NOVOS\\"
    python processar_lote_v2.py --fix 545234 545248
    python processar_lote_v2.py --resume  # Retoma do ultimo checkpoint
"""
import os, sys, json, time, shutil, re

# Fix #20: Force UTF-8 encoding on Windows
if sys.stdout.encoding != 'utf-8':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Load .env WITHOUT importing app.py (fix #21/#22 — no Flask/scheduler/monitor)
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env_path):
    with open(_env_path, encoding='utf-8') as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                _k = _k.strip()
                if _k == "ANTHROPIC_API_KEY" or _k not in os.environ:
                    os.environ[_k] = _v.strip()

# Fix #21/#22: Disable schedulers/monitors before importing app
os.environ['FOLLOWUP_ENABLED'] = 'false'
os.environ['MATERNIDADE_ENABLED'] = 'false'
os.environ['MONITOR_ENABLED'] = 'false'

from app import (merge_pdf_parts, detect_duplicates, organizar_pasta,
                 analisar_pasta_internal, gerar_documentos_internal,
                 detect_uf_from_folder, OUTPUT_DIR)
from legalmail_service import (LegalMailService, validar_cep,
                                completar_endereco_por_cep, buscar_cep_por_endereco,
                                UF_TRIBUNAL_MAP)
from cpfcnpj_service import consultar_cpf, gerar_declaracao_residencia

CHECKPOINT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'lote_checkpoint.json')


def save_checkpoint(completed, pending, errors, current):
    """Fix #23: Save progress so we can resume after crash."""
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump({
            'completed': completed,
            'pending': pending,
            'errors': errors,
            'current': current,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        }, f, ensure_ascii=False, indent=2)


def load_checkpoint():
    """Load previous checkpoint if exists."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, encoding='utf-8') as f:
            return json.load(f)
    return None


def extrair_dados_partes(extracted):
    """Extract party data from Haiku analysis + validate via ViaCEP.

    Returns dict with keys:
        representante: {nome, documento, endereco_*} or None
        menor: {nome, documento, endereco_*} or None
        autora: {nome, documento, endereco_*} or None (adult, no representative)
        cidade, uf: detected from address
    """
    # Names — try ALL possible field names (Haiku returns vary)
    rep_nome = (extracted.get('representante_nome', '')
                or extracted.get('nome_representante', '')
                or (extracted.get('representante', {}).get('nome', '')
                    if isinstance(extracted.get('representante'), dict) else ''))
    rep_cpf = (extracted.get('representante_cpf', '')
               or extracted.get('cpf_representante', ''))

    autor_nome = extracted.get('nome', '') or extracted.get('nome_autor', '')
    autor_cpf = extracted.get('cpf', '') or extracted.get('cpf_autor', '')

    # Se tem representante mas nao tem nome do autor, tentar extrair do nome da pasta
    if rep_nome and not autor_nome:
        # Nome do menor pode estar no nome da pasta: "NOME_MENOR_REPRESENTANTE"
        print(f"    [WARN] Autor sem nome mas tem representante '{rep_nome}' — verificar pasta")

    # Validacao: se tem representante mas sem CPF, alertar
    if rep_nome and not rep_cpf:
        print(f"    [WARN] Representante '{rep_nome}' sem CPF — sera extraido da peticao")
    if autor_nome and not autor_cpf:
        print(f"    [WARN] Autor '{autor_nome}' sem CPF — sera extraido da peticao")

    # Address
    cep = str(extracted.get('cep', '')).strip()
    cidade = str(extracted.get('cidade', '') or extracted.get('municipio', '')).strip()
    uf = str(extracted.get('estado', '') or extracted.get('uf', '')).strip()
    logradouro = str(extracted.get('logradouro', '') or extracted.get('rua', '')).strip()
    numero = str(extracted.get('numero', '')).strip() or 'S/N'
    bairro = str(extracted.get('bairro', '')).strip()

    # Parse "endereco" string if individual fields are empty
    endereco_str = extracted.get('endereco', '')
    if isinstance(endereco_str, str) and endereco_str and not logradouro:
        parts = [p.strip() for p in endereco_str.split(',')]
        if len(parts) >= 1:
            logradouro = parts[0]
        if len(parts) >= 2:
            n = parts[1].strip()
            if n.isdigit() or n.upper() == 'S/N':
                numero = n
            else:
                bairro = n
        if len(parts) >= 3:
            bairro = parts[-1]

    # ViaCEP: validate and complete address
    if cep:
        cep_clean = re.sub(r'\D', '', cep)
        viacep = validar_cep(cep_clean)
        if viacep:
            cep = cep_clean
            if not cidade:
                cidade = viacep.get('localidade', '')
            if not uf:
                uf = viacep.get('uf', '')
            if not bairro and viacep.get('bairro'):
                bairro = viacep['bairro']
            if not logradouro and viacep.get('logradouro'):
                logradouro = viacep['logradouro']
            print(f"    [CEP] {cep} válido → {cidade}/{uf}")
        else:
            print(f"    [CEP] {cep} INVÁLIDO — tentando buscar...")
            if uf and cidade and logradouro:
                novo = buscar_cep_por_endereco(uf, cidade, logradouro)
                if novo:
                    cep = novo
                    print(f"    [CEP] Encontrado: {novo}")
    elif uf and cidade and logradouro:
        # No CEP provided — search for it
        print(f"    [CEP] Buscando CEP para {logradouro}, {cidade}/{uf}...")
        novo = buscar_cep_por_endereco(uf, cidade, logradouro)
        if novo:
            cep = novo
            addr = completar_endereco_por_cep(novo)
            if addr:
                if not bairro:
                    bairro = addr.get('endereco_bairro', '')
            print(f"    [CEP] Encontrado: {novo}")

    # CPFCNPJ fallback: se falta CEP/endereco, buscar pela API
    cpf_consulta = rep_cpf or autor_cpf
    endereco_via_cpfcnpj = False
    if not cep and cpf_consulta:
        cpf_limpo = re.sub(r'\D', '', cpf_consulta)
        if len(cpf_limpo) == 11:
            print(f"    [CPFCNPJ] Sem endereco nos docs — consultando CPF {cpf_limpo[:3]}...{cpf_limpo[-2:]}...")
            try:
                dados_cpf = consultar_cpf(cpf_limpo)
                end = dados_cpf.get('endereco', {})
                if end.get('cep'):
                    cep = end['cep']
                    if not logradouro:
                        logradouro = end.get('logradouro', '')
                    if not numero or numero == 'S/N':
                        numero = end.get('numero', '') or 'S/N'
                    if not bairro:
                        bairro = end.get('bairro', '')
                    if not cidade:
                        cidade = end.get('cidade', '')
                    if not uf:
                        uf = end.get('uf', '')
                    endereco_via_cpfcnpj = True
                    print(f"    [CPFCNPJ] Endereco encontrado: {logradouro}, {numero} - {cidade}/{uf} CEP {cep}")
                else:
                    print(f"    [CPFCNPJ] CPF encontrado mas sem endereco na base")
            except Exception as e:
                print(f"    [CPFCNPJ] Falha na consulta: {e}")

    # Build address dict
    addr = {
        'endereco_cep': cep,
        'endereco_logradouro': logradouro,
        'endereco_numero': numero,
        'endereco_bairro': bairro,
        'endereco_cidade': cidade,
        'endereco_uf': uf,
    }

    result = {'cidade': cidade, 'uf': uf, 'endereco_via_cpfcnpj': endereco_via_cpfcnpj}

    if rep_nome and autor_nome:
        # Has representative (mãe/pai) + minor
        # Profissao default depende do parentesco: mae=Do Lar, pai/tutor=Desempregado
        parentesco = (extracted.get('representante_parentesco', '') or '').lower()
        if parentesco in ('pai', 'tutor', 'curador'):
            prof_default = 'Desempregado'
        else:
            prof_default = 'DO LAR\\DONA DE CASA'
        result['representante'] = {
            'nome': rep_nome,
            'documento': rep_cpf,
            'profissao': extracted.get('profissao_representante', '') or prof_default,
            **addr,
        }
        result['menor'] = {
            'nome': autor_nome,
            'documento': autor_cpf,
            'profissao': 'DESEMPREGADO',
            **addr,
        }
    elif autor_nome:
        # Adult (Denise, Roseli) — represents self
        result['autora'] = {
            'nome': autor_nome,
            'documento': autor_cpf,
            'profissao': extracted.get('profissao', '') or extracted.get('profissao_autor', '') or '',
            **addr,
        }

    return result


def processar_cliente(pasta, svc, skip_legalmail=False, dry_run=False):
    """Process ONE client completely. Returns result dict with per-stage timing."""
    nome = os.path.basename(pasta).split("_")[0]
    result = {'cliente': nome, 'pasta': pasta, 'timings': {}}
    t_start = time.time()

    def _stage(name):
        """Mark start of a stage for timing."""
        result['timings'][name] = time.time()

    def _stage_end(name):
        """Mark end of stage, print elapsed."""
        if name in result['timings']:
            elapsed = time.time() - result['timings'][name]
            result['timings'][name] = round(elapsed, 1)
            return elapsed
        return 0

    # Step 1: Organize folder
    _stage('organizar')
    print(f"  [1/5] Organizando pasta...")
    try:
        merge_pdf_parts(pasta)
        detect_duplicates(pasta)
        organizar_pasta(pasta)
    except Exception as e:
        print(f"    [WARN] Organização: {e}")
    _stage_end('organizar')

    # Step 2: OCR + Haiku analysis (with retry on transient failure)
    _stage('analise')
    print(f"  [2/5] Analisando documentos (OCR + Haiku)...")
    t0 = time.time()
    extracted = None
    for tentativa in range(2):
        try:
            extracted = analisar_pasta_internal(pasta)
            break
        except Exception as e:
            if tentativa == 0:
                print(f"    [RETRY] Analise falhou ({e}), tentando novamente...")
                time.sleep(5)
            else:
                raise
    # Validar que extracted tem dados minimos (bug #7)
    autor_n = extracted.get('nome', '') or extracted.get('nome_autor', '')
    rep_n = extracted.get('representante_nome', '') or extracted.get('nome_representante', '')
    autor_cpf_ok = extracted.get('cpf', '') or extracted.get('cpf_autor', '')
    rep_cpf_ok = extracted.get('representante_cpf', '') or extracted.get('cpf_representante', '')

    if not autor_n and not rep_n:
        print(f"    [WARN] Haiku nao extraiu NENHUM nome — dados podem estar incompletos")

    # Otimizacao: se tem representante mas falta CPF, tentar extrair focado nos docs de identidade
    if rep_n and not rep_cpf_ok:
        print(f"    [CPF-RETRY] Representante sem CPF, buscando nos docs de identidade...")
        for f in sorted(os.listdir(pasta)):
            fn = f.lower()
            if fn.endswith('.pdf') and any(kw in fn for kw in ['cpf', 'rg', 'identif', 'procura', '6-', '5-']):
                try:
                    from app import mistral_ocr
                    ocr_text = mistral_ocr(os.path.join(pasta, f))
                    if ocr_text:
                        cpfs_found = re.findall(r'\d{3}\.\d{3}\.\d{3}-\d{2}', ocr_text)
                        # Tambem buscar CPF sem formatacao
                        cpfs_raw = re.findall(r'(?<!\d)(\d{11})(?!\d)', ocr_text)
                        for raw in cpfs_raw:
                            fmt = f"{raw[:3]}.{raw[3:6]}.{raw[6:9]}-{raw[9:]}"
                            if fmt not in cpfs_found:
                                cpfs_found.append(fmt)
                        # Filtrar CPFs que nao sao do autor
                        for cpf in cpfs_found:
                            if cpf != autor_cpf_ok:
                                extracted['representante_cpf'] = cpf
                                rep_cpf_ok = cpf
                                print(f"    [CPF-RETRY] Encontrado: {cpf} em {f}")
                                break
                    if rep_cpf_ok:
                        break
                except Exception:
                    pass

    if autor_n and not autor_cpf_ok:
        print(f"    [WARN] Autor '{autor_n}' sem CPF")

    print(f"    Análise OK ({time.time()-t0:.0f}s)")
    print(f"    Autor: {autor_n or '?'} (CPF: {'OK' if autor_cpf_ok else 'FALTA'}) | Rep: {rep_n or '?'} (CPF: {'OK' if rep_cpf_ok else 'FALTA'})")
    _stage_end('analise')

    # Step 3: Extract + validate party data with ViaCEP
    _stage('validacao')
    print(f"  [3/5] Validando dados das partes (ViaCEP)...")
    party_data = extrair_dados_partes(extracted)
    result['uf'] = party_data.get('uf', '')
    result['cidade'] = party_data.get('cidade', '')

    _stage_end('validacao')

    # Step 3b: Se endereco veio da CPFCNPJ, gerar Declaracao de Residencia
    if party_data.get('endereco_via_cpfcnpj'):
        print(f"  [3b] Gerando Declaracao de Residencia (endereco via CPFCNPJ)...")
        try:
            pessoa = party_data.get('representante') or party_data.get('autora') or {}
            decl_path = gerar_declaracao_residencia(
                nome=pessoa.get('nome', ''),
                cpf=pessoa.get('documento', ''),
                logradouro=pessoa.get('endereco_logradouro', ''),
                numero=pessoa.get('endereco_numero', 'S/N'),
                bairro=pessoa.get('endereco_bairro', ''),
                cidade=pessoa.get('endereco_cidade', ''),
                uf=pessoa.get('endereco_uf', ''),
                cep=pessoa.get('endereco_cep', ''),
                output_dir=pasta,
            )
            print(f"    Declaracao salva: {os.path.basename(decl_path)}")
            result['declaracao_residencia'] = decl_path
        except Exception as e:
            print(f"    [WARN] Falha ao gerar declaracao: {e}")

    # Step 4: Generate 4 documents
    _stage('geracao')
    print(f"  [4/5] Gerando documentos (Sonnet)...")
    t0 = time.time()
    files = gerar_documentos_internal(extracted)
    print(f"    {len(files)} docs gerados ({time.time()-t0:.0f}s)")
    copied = 0
    for fname in files:
        src = os.path.join(OUTPUT_DIR, fname)
        if os.path.exists(src):
            shutil.copy2(src, os.path.join(pasta, fname))
            copied += 1
    result['docs_gerados'] = len(files)
    _stage_end('geracao')

    # Step 5: LegalMail — create draft, fill ALL fields, upload docs
    if not skip_legalmail:
        _stage('legalmail')
        print(f"  [5/5] LegalMail...")
        # Build client_data for the service
        client_data = {}
        if party_data.get('representante') and party_data.get('menor'):
            client_data = {
                'representante': party_data['representante'],
                'menor': party_data['menor'],
            }
        elif party_data.get('autora'):
            client_data = party_data['autora']

        # VALIDACAO PRE-LEGALMAIL: checar dados minimos antes de criar rascunho
        pre_erros = []
        if party_data.get('representante') and party_data.get('menor'):
            rep = party_data['representante']
            men = party_data['menor']
            if not men.get('nome'):
                pre_erros.append("Menor sem nome")
            if not rep.get('nome'):
                pre_erros.append("Representante sem nome")
            if not party_data.get('uf'):
                pre_erros.append("UF nao detectada")
            if not party_data.get('cidade'):
                pre_erros.append("Cidade nao detectada")
            cep_r = rep.get('endereco_cep', '')
            if not cep_r or len(re.sub(r'\D', '', cep_r)) != 8:
                pre_erros.append(f"CEP invalido: '{cep_r}'")
        elif party_data.get('autora'):
            if not party_data['autora'].get('nome'):
                pre_erros.append("Autora sem nome")
        else:
            pre_erros.append("Sem dados de partes")

        # Separar erros criticos (bloqueiam) de alertas (continuam)
        criticos = [e for e in pre_erros if 'sem nome' in e.lower() or 'sem dados' in e.lower()]
        alertas = [e for e in pre_erros if e not in criticos]

        if criticos:
            print(f"    [PRE-CHECK] BLOQUEADO: {', '.join(criticos)}")
            result['legalmail'] = {'status': 'erro', 'error': f"Pre-check: {', '.join(criticos)}"}
            result['tempo_total'] = round(time.time() - t_start, 1)
            result['status'] = 'erro'
            return result
        if alertas:
            print(f"    [PRE-CHECK] ALERTAS: {', '.join(alertas)}")
            # Continua com alertas — CEP/UF/cidade podem ser corrigidos pelo LegalMailService

        try:
            lm = svc.processar_cliente(pasta, client_data=client_data, extracted=extracted, dry_run=dry_run)
            result['legalmail'] = {
                'status': lm.get('status', '?'),
                'url': lm.get('url', ''),
                'uploaded': lm.get('uploaded', 0),
                'filled': lm.get('filled', []),
                'errors': lm.get('fill_errors', []),
            }
            if lm.get('url'):
                print(f"    URL: {lm['url']}")
        except Exception as e:
            print(f"    ERRO LegalMail: {e}")
            result['legalmail'] = {'status': 'erro', 'error': str(e)[:200]}
        _stage_end('legalmail')
    else:
        print(f"  [5/5] LegalMail PULADO")

    result['tempo_total'] = round(time.time() - t_start, 1)
    # Print timing summary
    timings = result.get('timings', {})
    if timings:
        parts = [f"{k}={v}s" for k, v in timings.items() if isinstance(v, (int, float))]
        print(f"    [TIMING] {' | '.join(parts)} | total={result['tempo_total']}s")
    # Status baseado no resultado REAL do LegalMail (bug #13)
    lm_status = result.get('legalmail', {}).get('status', '')
    if lm_status == 'erro':
        result['status'] = 'parcial'  # Docs gerados mas LegalMail falhou
    else:
        result['status'] = 'ok'
    return result


def fix_existing_drafts(svc, draft_ids):
    """Fix existing drafts that have missing fields."""
    from legalmail_service import fix_rascunho
    print(f"=== CORRIGINDO {len(draft_ids)} RASCUNHOS ===\n")

    for idpet in draft_ids:
        try:
            r = fix_rascunho(svc, int(idpet))
            print(f"  {idpet}: {r.get('filled', [])} | erros={r.get('errors', [])}")
        except Exception as e:
            print(f"  {idpet}: ERRO {e}")


def classify_tribunal(pasta):
    """Fix #24: Determine if case is PJe or eProc based on folder UF.
    Returns ('pje', uf_info) or ('eproc', uf_info)."""
    uf = detect_uf_from_folder(pasta)
    if not uf:
        return 'pje', {}  # default
    info = UF_TRIBUNAL_MAP.get(uf, {})
    sistema = info.get('sistema', 'pje')
    is_eproc = 'eproc' in sistema
    return ('eproc' if is_eproc else 'pje'), info


def validate_pasta_pre(pasta):
    """Fix #25: Pre-validate folder data before calling any API.
    Returns list of issues found."""
    issues = []
    nome = os.path.basename(pasta).split("_")[0]

    # Check PDFs exist
    pdfs = [f for f in os.listdir(pasta) if f.lower().endswith('.pdf')]
    if len(pdfs) < 3:
        issues.append(f"Poucos PDFs ({len(pdfs)})")

    # Check for minimum docs
    has_procuracao = any('procura' in f.lower() for f in pdfs)
    has_doc = any(f.lower().startswith('5-') or f.lower().startswith('6-') for f in pdfs)
    if not has_procuracao:
        issues.append("Sem procuracao")
    if not has_doc:
        issues.append("Sem documento de identidade")

    return issues


def main():
    svc = LegalMailService()

    # Parse args
    args = sys.argv[1:]

    # Mode: fix existing drafts
    if '--fix' in args:
        args.remove('--fix')
        fix_existing_drafts(svc, args)
        return

    # Mode: resume from checkpoint (fix #23)
    if '--resume' in args:
        cp = load_checkpoint()
        if not cp:
            print("Sem checkpoint para retomar.")
            sys.exit(1)
        completed = set(cp.get('completed', []))
        print(f"=== RETOMANDO: {len(cp['pending'])} pendentes, {len(completed)} ja feitos ===\n")
        args = cp['pending']

    # Mode: process all subfolders in a base directory
    if '--pasta-base' in args:
        idx = args.index('--pasta-base')
        base = args[idx + 1]
        args = [os.path.join(base, d) for d in os.listdir(base)
                if os.path.isdir(os.path.join(base, d))]

    # Mode: process specific folders
    if not args:
        print("Uso:")
        print("  python processar_lote_v2.py PASTA1 PASTA2 ...")
        print('  python processar_lote_v2.py --pasta-base "H:\\Meu Drive\\JUDICIAIS\\NOVOS\\"')
        print("  python processar_lote_v2.py --fix 545234 545248")
        print("  python processar_lote_v2.py --resume  # Retoma do checkpoint")
        print("  python processar_lote_v2.py --dry-run PASTA  # Cria rascunho sem upload")
        print("  python processar_lote_v2.py --no-legalmail PASTA  # Gera docs sem LegalMail")
        sys.exit(1)

    skip_lm = '--no-legalmail' in args
    dry_run = '--dry-run' in args  # Cria rascunho + preenche mas NAO faz upload
    args = [a for a in args if not a.startswith('--')]

    # Fix #24: Separate PJe and eProc queues
    pje_queue = []
    eproc_queue = []
    for pasta in args:
        if not os.path.isdir(pasta):
            continue
        tipo, info = classify_tribunal(pasta)
        if tipo == 'eproc':
            eproc_queue.append(pasta)
        else:
            pje_queue.append(pasta)

    print(f"=== PROCESSAMENTO v3: {len(args)} CLIENTES ===")
    print(f"    PJe: {len(pje_queue)} | eProc: {len(eproc_queue)}")

    # Fix #25: Pre-validate all folders
    print(f"\n--- VALIDACAO PREVIA ---")
    for pasta in args:
        if os.path.isdir(pasta):
            issues = validate_pasta_pre(pasta)
            nome = os.path.basename(pasta).split("_")[0]
            if issues:
                print(f"  [WARN] {nome}: {', '.join(issues)}")

    print(f"\n--- PROCESSANDO PJe ({len(pje_queue)}) ---\n")
    results = []
    completed_names = []
    total_start = time.time()

    # Process PJe first (simpler flow)
    all_ordered = pje_queue + eproc_queue

    # Show separator when switching from PJe to eProc
    eproc_start = len(pje_queue)

    # Signal handler: salvar checkpoint se Ctrl+C
    import signal
    def _save_on_interrupt(signum, frame):
        print(f"\n  [CTRL+C] Salvando checkpoint antes de sair...")
        pending = [p for p in all_ordered[len(completed_names):]]
        save_checkpoint(completed_names, pending, [], 'INTERRUPTED')
        print(f"  Checkpoint salvo. Use --resume pra continuar.")
        sys.exit(1)
    signal.signal(signal.SIGINT, _save_on_interrupt)

    for i, pasta in enumerate(all_ordered):
        nome = os.path.basename(pasta).split("_")[0]

        if i == eproc_start and eproc_queue:
            print(f"\n--- PROCESSANDO eProc ({len(eproc_queue)}) ---\n")

        if not os.path.isdir(pasta):
            print(f"[{i+1}/{len(all_ordered)}] {nome}: PASTA NAO ENCONTRADA\n")
            results.append({'cliente': nome, 'status': 'skip'})
            continue

        print(f"[{i+1}/{len(all_ordered)}] {nome}")
        try:
            r = processar_cliente(pasta, svc, skip_legalmail=skip_lm, dry_run=dry_run)
            results.append(r)
            completed_names.append(nome)
            print(f"  CONCLUIDO em {r['tempo_total']}s\n")
        except Exception as e:
            print(f"  ERRO FATAL: {e}\n")
            import traceback
            traceback.print_exc()
            results.append({'cliente': nome, 'status': 'erro', 'error': str(e)[:300]})

        # Fix #23: Save checkpoint after each client
        pending = [p for p in all_ordered[i+1:]]
        save_checkpoint(completed_names, pending, [r.get('error','') for r in results if r.get('status')=='erro'], nome)

        if i < len(all_ordered) - 1:
            time.sleep(3)

    total = time.time() - total_start

    # Bug #6: Deletar checkpoint apos conclusao bem-sucedida
    if os.path.exists(CHECKPOINT_FILE):
        try:
            os.remove(CHECKPOINT_FILE)
            print(f"  Checkpoint removido (lote concluido)")
        except OSError:
            pass  # Arquivo pode estar em uso

    # Save results JSON
    with open('lote_resultado_v2.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # Save results CSV (relatório batch)
    from datetime import datetime as _dt
    csv_file = f"relatorios/{_dt.now().strftime('%Y-%m-%d')}_RESUMO_BATCH.csv"
    os.makedirs("relatorios", exist_ok=True)
    try:
        import csv
        with open(csv_file, 'w', newline='', encoding='utf-8-sig') as cf:
            writer = csv.writer(cf, delimiter=';')
            writer.writerow(['Cliente', 'Status', 'Tempo(s)', 'LegalMail URL', 'Erros Upload', 'Pendencias', 'Erro'])
            for r in results:
                nome = r.get('cliente', '?')
                status = r.get('status', '?')
                tempo = r.get('tempo_total', 0)
                lm = r.get('legalmail', {})
                url = lm.get('url', '')
                errs = '; '.join(lm.get('errors', []))[:200]
                pendencias = ''
                # Verificar pendencias
                pasta_cliente = r.get('pasta', '')
                pend_file = os.path.join(pasta_cliente, 'PENDENCIAS_UPLOAD.json') if pasta_cliente else ''
                if pend_file and os.path.exists(pend_file):
                    try:
                        with open(pend_file, encoding='utf-8') as pf:
                            pdata = json.load(pf)
                            pendencias = '; '.join([p.get('tipo', '') for p in pdata.get('pendencias', [])])
                    except Exception:
                        pass
                erro = r.get('error', '')[:200]
                writer.writerow([nome, status, tempo, url, errs, pendencias, erro])
        print(f"\n  Relatório CSV: {csv_file}")
    except Exception as e:
        print(f"\n  [WARN] Erro ao gerar CSV: {e}")

    # Save pendencias consolidadas
    todas_pendencias = []
    for r in results:
        pasta_cliente = r.get('pasta', '')
        if pasta_cliente:
            pend_file = os.path.join(pasta_cliente, 'PENDENCIAS_UPLOAD.json')
            if os.path.exists(pend_file):
                try:
                    with open(pend_file, encoding='utf-8') as pf:
                        pdata = json.load(pf)
                        for p in pdata.get('pendencias', []):
                            p['caso'] = r.get('cliente', '?')
                            todas_pendencias.append(p)
                except Exception:
                    pass
    if todas_pendencias:
        pend_global = f"relatorios/{_dt.now().strftime('%Y-%m-%d')}_PENDENCIAS_MANUAIS.json"
        with open(pend_global, 'w', encoding='utf-8') as pf:
            json.dump({"pendencias": todas_pendencias, "total": len(todas_pendencias)}, pf, ensure_ascii=False, indent=2)
        print(f"  Pendencias manuais: {pend_global} ({len(todas_pendencias)} itens)")

    # Summary
    print(f"\n{'='*60}")
    print(f"  RESUMO ({total:.0f}s total)")
    print(f"{'='*60}")
    ok = 0
    parcial = 0
    for r in results:
        nome = r.get('cliente', '?')
        status = r.get('status', '?')
        lm = r.get('legalmail', {})
        url = lm.get('url', '')
        errs = len(lm.get('errors', []))
        tempo = r.get('tempo_total', 0)
        if status == 'ok':
            ok += 1
            err_str = f" ({errs} erros)" if errs else ""
            val_errs = r.get('legalmail', {}).get('validacao_erros', [])
            val_str = f" [VALIDACAO: {','.join(val_errs)}]" if val_errs else ""
            print(f"  OK  {nome} ({tempo}s){err_str}{val_str} -> {url}")
        elif status == 'parcial':
            parcial += 1
            print(f"  PAR {nome}: docs gerados, LegalMail falhou — {r.get('legalmail', {}).get('error', '')[:80]}")
        else:
            print(f"  ERR {nome}: {r.get('error', status)[:80]}")
    print(f"\n  {ok}/{len(results)} OK | {parcial} parcial | {len(results)-ok-parcial} erro")


if __name__ == '__main__':
    main()
