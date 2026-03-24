"""
PROCESSAMENTO COMPLETO EM LOTE v2 — BPC/LOAS
Um comando, tudo automático, máx 5 min por cliente:

1. Organiza pasta (merge, duplicados, renomear)
2. OCR + análise (extrai dados 1x só — nome, CPF, endereço, cidade, UF)
3. Valida/completa endereço via ViaCEP
4. Gera 4 documentos (petição, planilha, quesitos médicos, quesitos sociais)
5. Converte .docx → .pdf se necessário
6. Cria rascunho no LegalMail com TODOS os campos preenchidos

Uso:
    python processar_lote_v2.py "H:\\Meu Drive\\JUDICIAIS\\NOVOS\\CLIENTE1" "CLIENTE2" ...
    python processar_lote_v2.py --pasta-base "H:\\Meu Drive\\JUDICIAIS\\NOVOS\\"
    python processar_lote_v2.py --fix 545234 545248    # Corrige rascunhos existentes
"""
import os, sys, json, time, shutil, re

# Fix Windows cp1252 encoding issues with unicode chars
if sys.stdout.encoding != 'utf-8':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Load .env
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

from app import (merge_pdf_parts, detect_duplicates, organizar_pasta,
                 analisar_pasta_internal, gerar_documentos_internal,
                 detect_uf_from_folder, OUTPUT_DIR)
from legalmail_service import (LegalMailService, validar_cep,
                                completar_endereco_por_cep, buscar_cep_por_endereco)


def extrair_dados_partes(extracted):
    """Extract party data from Haiku analysis + validate via ViaCEP.

    Returns dict with keys:
        representante: {nome, documento, endereco_*} or None
        menor: {nome, documento, endereco_*} or None
        autora: {nome, documento, endereco_*} or None (adult, no representative)
        cidade, uf: detected from address
    """
    # Names
    rep_nome = (extracted.get('nome_representante', '')
                or extracted.get('representante_nome', '')
                or (extracted.get('representante', {}).get('nome', '')
                    if isinstance(extracted.get('representante'), dict) else ''))
    rep_cpf = (extracted.get('cpf_representante', '')
               or extracted.get('representante_cpf', ''))

    autor_nome = extracted.get('nome_autor', '') or extracted.get('nome', '')
    autor_cpf = extracted.get('cpf_autor', '') or extracted.get('cpf', '')

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

    # Build address dict
    addr = {
        'endereco_cep': cep,
        'endereco_logradouro': logradouro,
        'endereco_numero': numero,
        'endereco_bairro': bairro,
        'endereco_cidade': cidade,
        'endereco_uf': uf,
    }

    result = {'cidade': cidade, 'uf': uf}

    if rep_nome and autor_nome:
        # Has representative (mãe/pai) + minor
        result['representante'] = {
            'nome': rep_nome,
            'documento': rep_cpf,
            'profissao': extracted.get('profissao_representante', '') or 'DO LAR\\DONA DE CASA',
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


def processar_cliente(pasta, svc, skip_legalmail=False):
    """Process ONE client completely. Returns result dict."""
    nome = os.path.basename(pasta).split("_")[0]
    result = {'cliente': nome, 'pasta': pasta}
    t_start = time.time()

    # Step 1: Organize folder
    print(f"  [1/5] Organizando pasta...")
    try:
        merge_pdf_parts(pasta)
        detect_duplicates(pasta)
        organizar_pasta(pasta)
    except Exception as e:
        print(f"    [WARN] Organização: {e}")

    # Step 2: OCR + Haiku analysis (ONE pass)
    print(f"  [2/5] Analisando documentos (OCR + Haiku)...")
    t0 = time.time()
    extracted = analisar_pasta_internal(pasta)
    print(f"    Análise OK ({time.time()-t0:.0f}s)")
    print(f"    Autor: {extracted.get('nome_autor', '?')} | Rep: {extracted.get('nome_representante', '?')}")

    # Step 3: Extract + validate party data with ViaCEP
    print(f"  [3/5] Validando dados das partes (ViaCEP)...")
    party_data = extrair_dados_partes(extracted)
    result['uf'] = party_data.get('uf', '')
    result['cidade'] = party_data.get('cidade', '')

    # Step 4: Generate 4 documents
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

    # Step 5: LegalMail — create draft, fill ALL fields, upload docs
    if not skip_legalmail:
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

        try:
            lm = svc.processar_cliente(pasta, client_data=client_data, extracted=extracted)
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
    else:
        print(f"  [5/5] LegalMail PULADO")

    result['tempo_total'] = round(time.time() - t_start, 1)
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


def main():
    svc = LegalMailService()

    # Parse args
    args = sys.argv[1:]

    # Mode: fix existing drafts
    if '--fix' in args:
        args.remove('--fix')
        fix_existing_drafts(svc, args)
        return

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
        sys.exit(1)

    skip_lm = '--no-legalmail' in args
    args = [a for a in args if not a.startswith('--')]

    print(f"=== PROCESSAMENTO v2: {len(args)} CLIENTES ===\n")
    results = []
    total_start = time.time()

    for i, pasta in enumerate(args):
        nome = os.path.basename(pasta).split("_")[0]
        if not os.path.isdir(pasta):
            print(f"[{i+1}/{len(args)}] {nome}: PASTA NÃO ENCONTRADA\n")
            results.append({'cliente': nome, 'status': 'skip'})
            continue

        print(f"[{i+1}/{len(args)}] {nome}")
        try:
            r = processar_cliente(pasta, svc, skip_legalmail=skip_lm)
            results.append(r)
            print(f"  CONCLUÍDO em {r['tempo_total']}s\n")
        except Exception as e:
            print(f"  ERRO FATAL: {e}\n")
            import traceback
            traceback.print_exc()
            results.append({'cliente': nome, 'status': 'erro', 'error': str(e)[:300]})

        if i < len(args) - 1:
            time.sleep(3)

    total = time.time() - total_start

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
            print(f"  OK  {nome} ({tempo}s){err_str} → {url}")
        else:
            print(f"  ERR {nome}: {r.get('error', status)[:80]}")
    print(f"\n  {ok}/{len(results)} processados com sucesso")


if __name__ == '__main__':
    main()
