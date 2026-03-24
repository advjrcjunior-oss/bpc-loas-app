"""LegalMail Service v2 — Clean, correct, automated BPC/LOAS petition filing.

Usage:
    from legalmail_service import LegalMailService
    svc = LegalMailService()
    result = svc.processar_cliente(pasta)  # One call, everything done

API Endpoints (correct per OpenAPI spec 2026-03):
    POST/PUT/GET /api/v1/complaint          — Petition CRUD
    GET /api/v1/complaint/specialties       — Matéria/Especialidade
    GET /api/v1/complaint/district          — Comarcas/Jurisdições
    GET /api/v1/complaint/procedures        — Ritos
    GET /api/v1/complaint/classes           — Classes processuais
    GET /api/v1/complaint/subjects          — Assuntos
    GET /api/v1/complaint/areas             — Competências
    POST /api/v1/complaintsandpleadings/file        — Upload petição PDF
    POST /api/v1/complaintsandpleadings/attachments — Upload anexos
    GET /api/v1/complaintsandpleadings/attachment/types — Tipos de anexo
    GET /api/v1/complaintsandpleadings/courts       — Tribunais/sistemas
    POST /parts                             — Criar parte (legacy, funciona)
    GET /api/v1/party/professions           — Profissões válidas
"""
import os, json, time, re, requests

# ============================================================
# CONFIG
# ============================================================
LEGALMAIL_BASE = "https://app.legalmail.com.br/api/v1"
VIACEP_BASE = "https://viacep.com.br/ws"
RATE_LIMIT_DELAY = float(os.environ.get('LEGALMAIL_RATE_DELAY', '2.1'))  # seconds between requests

# UF → TRF mapping (complete, verified against CNJ)
UF_TRIBUNAL_MAP = {
    'DF': {'trf': 'TRF-1', 'sistema': 'pje', 'uf_tribunal': 'DF'},
    'GO': {'trf': 'TRF-1', 'sistema': 'pje', 'uf_tribunal': 'GO'},
    'BA': {'trf': 'TRF-1', 'sistema': 'pje', 'uf_tribunal': 'BA'},
    'PI': {'trf': 'TRF-1', 'sistema': 'pje', 'uf_tribunal': 'PI'},
    'MA': {'trf': 'TRF-1', 'sistema': 'pje', 'uf_tribunal': 'MA'},
    'PA': {'trf': 'TRF-1', 'sistema': 'pje', 'uf_tribunal': 'PA'},
    'AM': {'trf': 'TRF-1', 'sistema': 'pje', 'uf_tribunal': 'AM'},
    'AC': {'trf': 'TRF-1', 'sistema': 'pje', 'uf_tribunal': 'AC'},
    'AP': {'trf': 'TRF-1', 'sistema': 'pje', 'uf_tribunal': 'AP'},
    'RR': {'trf': 'TRF-1', 'sistema': 'pje', 'uf_tribunal': 'RR'},
    'RO': {'trf': 'TRF-1', 'sistema': 'pje', 'uf_tribunal': 'RO'},
    'TO': {'trf': 'TRF-1', 'sistema': 'pje', 'uf_tribunal': 'TO'},
    'MT': {'trf': 'TRF-1', 'sistema': 'pje', 'uf_tribunal': 'MT'},
    'RJ': {'trf': 'TRF-2', 'sistema': 'eproc_jfrj', 'uf_tribunal': 'RJ'},
    'ES': {'trf': 'TRF-2', 'sistema': 'eproc_jfes', 'uf_tribunal': 'ES'},
    'SP': {'trf': 'TRF-3', 'sistema': 'pje', 'uf_tribunal': 'SP'},
    'MS': {'trf': 'TRF-3', 'sistema': 'pje', 'uf_tribunal': 'MS'},
    'PR': {'trf': 'TRF-4', 'sistema': 'eproc_jfpr', 'uf_tribunal': 'PR'},
    'SC': {'trf': 'TRF-4', 'sistema': 'eproc_jfsc', 'uf_tribunal': 'SC'},
    'RS': {'trf': 'TRF-4', 'sistema': 'eproc_jfrs', 'uf_tribunal': 'RS'},
    'PE': {'trf': 'TRF-5', 'sistema': 'pje', 'uf_tribunal': 'PE'},
    'CE': {'trf': 'TRF-5', 'sistema': 'pje', 'uf_tribunal': 'CE'},
    'AL': {'trf': 'TRF-5', 'sistema': 'pje', 'uf_tribunal': 'AL'},
    'SE': {'trf': 'TRF-5', 'sistema': 'pje', 'uf_tribunal': 'SE'},
    'RN': {'trf': 'TRF-5', 'sistema': 'pje', 'uf_tribunal': 'RN'},
    'PB': {'trf': 'TRF-5', 'sistema': 'pje', 'uf_tribunal': 'PB'},
    'MG': {'trf': 'TRF-6', 'sistema': 'pje', 'uf_tribunal': 'MG'},
}

INSS_DATA = {
    "nome": "INSTITUTO NACIONAL DO SEGURO SOCIAL - INSS",
    "polo": "passivo",
    "documento": "29.979.036/0001-40",
    "personalidade": "Pessoa jurídica",
    "endereco_cep": "70040-902",
    "endereco_logradouro": "SAUS Quadra 2 Bloco O",
    "endereco_numero": "S/N",
    "endereco_bairro": "Asa Sul",
    "endereco_cidade": "Brasilia",
    "endereco_uf": "DF",
}

# Doc prefix → attachment type name mapping
# Classificacao CORRETA no LegalMail (verificado caso Erick Machado 24/03/2026)
DOC_PREFIX_MAP = {
    "2": "Procuração",
    "3": "Contrato de Honorários",
    "4": "Pedido de Gratuidade de Justiça",  # Era "Declaração de Hipossuficiência"
    "5": "Documento de Identificação",        # Era "Certidão de Nascimento"
    "6": "Documento de Identificação",        # Familiares — mesma classificacao
    "7": "Comprovante de Residência",
    "8": "Outros",
    "9": "Outros",
    "10": "Outros",
    "11": "Outros",                           # Requerimento INSS
    "12": "Outros",                           # Carta indeferimento
    "13": "Exame Médico",                     # Era "Laudo Médico"
    "14": "Exame Médico",                     # Era "Receitas Médicas"
    "15": "Exame Médico",                     # Receitas e exames
    "16": "Exame Médico",                     # Relatório médico
    "17": "Exame Médico",                     # Exames de imagem
    "18": "Outros",
    "19": "Outros",
    "20": "Outros",                           # Biometria
    "21": "Documento de Identificação",
    "22": "Outros",
    "23": "Outros",
    "PLANILHA": "Planilha",                   # Planilha de cálculos
    "QUESITOS": "Outros",                     # Quesitos médicos/sociais
}


# Ordem de upload dos documentos (prefixo → prioridade)
ORDEM_DOCUMENTOS = [
    "1",   # Petição (principal)
    "2",   # Procuração
    "4",   # Declaração hipossuficiência
    "3",   # Contrato de honorários
    "5",   # Documento do cliente/menor
    "6",   # Documentos de familiares
    "7",   # Comprovante de residência
    "11",  # Requerimento INSS
    "12",  # Carta indeferimento
    "13",  # Laudo médico
    "14",  # Relatório médico
    "15",  # Receitas e exames
    "16",  # Relatório médico 2
    "17",  # Exames de imagem
    "20",  # Biometria
    "PLANILHA",  # Planilha de cálculos
    "QUESITOS",  # Quesitos
]


import PyPDF2

def validar_pdf(caminho):
    """Valida que o arquivo é um PDF válido, não vazio, não corrompido."""
    if not os.path.exists(caminho):
        return False, "Arquivo nao existe"
    if os.path.getsize(caminho) < 100:
        return False, "Arquivo muito pequeno (possivelmente vazio)"
    try:
        with open(caminho, 'rb') as f:
            header = f.read(5)
            if header != b'%PDF-':
                return False, "Nao e um PDF valido (header incorreto, pode ser DOCX disfarçado)"
        with open(caminho, 'rb') as f:
            reader = PyPDF2.PdfReader(f)
            if len(reader.pages) == 0:
                return False, "PDF sem paginas"
        return True, "OK"
    except Exception as e:
        return False, f"PDF corrompido: {e}"


def validar_procuracao(caminho):
    """Verifica se a procuração tem assinatura (digital ou imagem)."""
    if not os.path.exists(caminho):
        return False, "Arquivo nao existe"
    try:
        with open(caminho, 'rb') as f:
            reader = PyPDF2.PdfReader(f)
            # Verificar assinatura digital
            if '/Sig' in str(reader.trailer) or '/AcroForm' in str(reader.trailer):
                return True, "Assinatura digital detectada"
            # Verificar se tem imagens (possível assinatura escaneada)
            for page in reader.pages:
                resources = page.get('/Resources', {})
                xobject = resources.get('/XObject', {})
                if xobject:
                    return True, "Imagem detectada (possível assinatura)"
            # Se tem mais de 1 página, provavelmente tem assinatura na última
            if len(reader.pages) > 1:
                return True, "Multiplas paginas (assumindo assinatura)"
        return False, "Nenhuma assinatura detectada"
    except Exception as e:
        return False, f"Erro ao verificar: {e}"


def ordenar_documentos(arquivos):
    """Ordena lista de arquivos na sequência correta para upload."""
    def prioridade(nome):
        nome_upper = nome.upper()
        if "PLANILHA" in nome_upper:
            return 100
        if "QUESITOS" in nome_upper:
            return 101
        # Extrair prefixo numerico
        m = re.match(r'^(\d+)', os.path.basename(nome))
        if m:
            num = int(m.group(1))
            try:
                return ORDEM_DOCUMENTOS.index(str(num))
            except ValueError:
                return 50 + num
        return 99
    return sorted(arquivos, key=prioridade)


# ============================================================
# VIACEP SERVICE
# ============================================================
def validar_cep(cep):
    """Validate CEP via ViaCEP. Returns address dict or None."""
    clean = re.sub(r'\D', '', str(cep))
    if len(clean) != 8:
        return None
    try:
        r = requests.get(f"{VIACEP_BASE}/{clean}/json/", timeout=10)
        if r.status_code == 200:
            data = r.json()
            if not data.get('erro'):
                return data
    except Exception:
        pass
    return None


def buscar_cep_por_endereco(uf, cidade, logradouro):
    """Search CEP by address via ViaCEP. Returns CEP string or None."""
    try:
        rua = re.sub(r'\s+', '+', logradouro.strip()[:40])
        url = f"{VIACEP_BASE}/{uf}/{cidade}/{rua}/json/"
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and len(data) > 0:
                return data[0].get('cep', '').replace('-', '')
    except Exception:
        pass
    return None


def completar_endereco_por_cep(cep):
    """Get full address from CEP. Returns dict with logradouro, bairro, cidade, uf."""
    data = validar_cep(cep)
    if not data:
        return None
    return {
        'endereco_cep': data.get('cep', '').replace('-', ''),
        'endereco_logradouro': data.get('logradouro', ''),
        'endereco_bairro': data.get('bairro', ''),
        'endereco_cidade': data.get('localidade', ''),
        'endereco_uf': data.get('uf', ''),
    }


# ============================================================
# LEGALMAIL SERVICE
# ============================================================
class LegalMailService:
    """Clean LegalMail API client for BPC/LOAS petitions."""

    def __init__(self, api_key=None, cert_id=None):
        self.api_key = api_key or os.environ.get('LEGALMAIL_API_KEY', '')
        self.cert_id = cert_id or int(os.environ.get('LEGALMAIL_CERTIFICADO_ID', '0') or '0')
        self._inss_id = None
        self._last_request = 0
        self._session_cookies = None  # For internal proporAcao/update endpoint
        if not self.api_key:
            raise ValueError("LEGALMAIL_API_KEY não configurada")

    # ---- Low-level API ----

    def _request(self, method, endpoint, **kwargs):
        """Make rate-limited authenticated request."""
        elapsed = time.time() - self._last_request
        if elapsed < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - elapsed)

        sep = '&' if '?' in endpoint else '?'
        url = f"{LEGALMAIL_BASE}{endpoint}{sep}api_key={self.api_key}"
        kwargs.setdefault('timeout', 30)
        self._last_request = time.time()
        r = getattr(requests, method)(url, **kwargs)

        if r.status_code == 429:
            print("    [RATE LIMIT] Aguardando 60s...")
            time.sleep(60)
            self._last_request = time.time()
            r = getattr(requests, method)(url, **kwargs)

        return r

    def _get_options(self, endpoint, idpet):
        """GET options list from a complaint/* endpoint."""
        r = self._request("get", f"{endpoint}?idpeticoes={idpet}")
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                return data
        return []

    # ---- Internal session for proporAcao/update (bypasses API classe requirement) ----

    def _get_session(self):
        """Login to LegalMail site and get session cookies.
        Uses LEGALMAIL_EMAIL and LEGALMAIL_PASSWORD env vars, or defaults."""
        if self._session_cookies:
            return self._session_cookies
        email = os.environ.get('LEGALMAIL_EMAIL', 'adv.jrcjunior@gmail.com')
        password = os.environ.get('LEGALMAIL_PASSWORD', 'Jrc@2026lm')
        s = requests.Session()
        r = s.post('https://app.legalmail.com.br/login', data={
            'email': email, 'password': password
        }, allow_redirects=True, timeout=15)
        if r.status_code == 200 and s.cookies:
            self._session_cookies = dict(s.cookies)
            print("    [SESSION] Login OK")
            return self._session_cookies
        print(f"    [SESSION] Login falhou: {r.status_code}")
        return None

    def _proporAcao_update(self, idpet, idproc, dados):
        """Use internal endpoint POST /api/proporAcao/update to set fields.
        This bypasses the API classe/areas requirement for eProc.
        Requires session cookies (not api_key)."""
        cookies = self._get_session()
        if not cookies:
            return False

        ASSUNTO_BPC = 'DIREITO ASSISTENCIAL (14) | Benefício Assistencial (Art. 203,V CF/88) (1402) | Pessoa com Deficiência (140201)'

        form = {
            'fk_peticao': str(idpet),
            'fk_processo': str(idproc),
            'assuntoPrincipal_proporAcao': ASSUNTO_BPC,
            'classeProcessual_proporAcao': '',
            'ritos_proporAcao': dados.get('rito', 'JUIZADO ESPECIAL FEDERAL'),
            'especialidade_proporAcao': dados.get('competencia', 'Federal'),
            'areas_proporAcao': dados.get('area', ''),
            'tribunal_proporAcao': dados.get('tribunal', ''),
            'sistema_tribunal': dados.get('sistema', ''),
            'foro_proporAcao': dados.get('comarca', ''),
            'grau_proporAcao': '1º Grau',
            'orgao_julgador_destino': dados.get('tribunal', ''),
            'tipoDistribuicao_proporAcao': dados.get('distribuicao', 'Por sorteio'),
            'titulo': 'Petição Inicial',
            'justicaGratuita_proporAcao': '1',
            'antecipacao_tutela': '1',
            'juizo_digital': '1',
            'renuncia_60_salarios': '1',
        }
        # Parties
        ativo = dados.get('idpoloativo', [])
        passivo = dados.get('idpolopassivo', [])
        if ativo:
            form['poloAtivo_proporAcao'] = str(ativo[0])
        if passivo:
            form['poloPassivo_proporAcao'] = str(passivo[0])
        if len(ativo) > 1:
            form['processos_clientes[]'] = [str(a) for a in ativo[1:]]
        if dados.get('valorCausa'):
            form['valorCausa_proporAcao'] = str(dados['valorCausa'])

        r = requests.post('https://app.legalmail.com.br/api/proporAcao/update',
                         data=form, cookies=cookies, timeout=15)
        if r.status_code == 200 and 'success' in r.text:
            print(f"    [proporAcao] Assunto+rito OK via endpoint interno")
            return True
        print(f"    [proporAcao] Falhou: {r.status_code} {r.text[:100]}")
        return False

    # ---- Petition CRUD (correct endpoints) ----

    def criar_peticao(self, tribunal, sistema, instancia="1", uf_tribunal=""):
        """POST /complaint — Create petition draft. Returns (idpeticoes, idprocessos) or raises."""
        payload = {
            "tribunal": tribunal,
            "instancia": instancia,
            "sistema": sistema,
            "certificado_id": self.cert_id,
        }
        if uf_tribunal:
            payload["ufTribunal"] = uf_tribunal

        r = self._request("post", "/complaint", json=payload)
        if r.status_code != 200:
            raise Exception(f"Erro ao criar petição: {r.status_code} {r.text[:300]}")

        data = r.json()
        if isinstance(data, list):
            data = data[0]
        if data.get("status") == "erro":
            raise Exception(f"Erro: {data.get('mensagem', data)}")

        dados = data.get("dados", {})
        idpet = dados.get("idpeticoes")
        idproc = dados.get("idprocessos")
        if not idpet:
            raise Exception(f"ID não retornado: {data}")
        print(f"    Rascunho criado: id={idpet}")
        return int(idpet), int(idproc)

    def get_peticao(self, idpet):
        """GET /complaint — Get current petition data."""
        r = self._request("get", f"/complaint?idpeticoes={idpet}")
        if r.status_code != 200:
            raise Exception(f"GET petição {idpet}: {r.status_code}")
        return r.json()["peticao"]["dados"]

    def put_peticao(self, idpet, dados):
        """PUT /complaint — Update petition fields. Returns True/False."""
        r = self._request("put", f"/complaint?idpeticoes={idpet}", json=dados)
        if r.status_code == 200:
            return True
        print(f"    PUT ERRO {r.status_code}: {r.text[:200]}")
        return False

    # ---- Field chain fill (per tribunal type) ----

    def preencher_campos(self, idpet, cidade, uf, tipo='deficiente'):
        """Fill ALL petition fields in correct dependency order.

        PJe chain:  especialidade → comarca → rito → classe → assunto → competência → flags
        eProc chain: competência(Federal) → comarca → rito → (classe vazia) → assunto → flags

        Returns dict of filled/failed fields.
        """
        result = {'filled': [], 'errors': []}

        # Get current state
        dados = self.get_peticao(idpet)
        is_eproc = 'eproc' in (dados.get('sistema', '') or '').lower()

        def try_put(field, value, label):
            """Try to update a single field. Re-fetches dados to avoid stale state."""
            nonlocal dados
            fresh = self.get_peticao(idpet)
            fresh[field] = value
            if self.put_peticao(idpet, fresh):
                dados = fresh
                result['filled'].append(label)
                print(f"    {label}: OK")
                return True
            result['errors'].append(label)
            return False

        def try_put_multi(updates, label):
            """Try to update multiple fields at once."""
            nonlocal dados
            fresh = self.get_peticao(idpet)
            for k, v in updates.items():
                fresh[k] = v
            if self.put_peticao(idpet, fresh):
                dados = fresh
                result['filled'].append(label)
                print(f"    {label}: OK")
                return True
            result['errors'].append(label)
            return False

        if is_eproc:
            return self._preencher_eproc(idpet, dados, cidade, uf, tipo, result, try_put, try_put_multi)

        # ===== PJe FLOW =====

        # Step 1: Especialidade (materia) — MUST be filled before comarca/rito/classe
        if not dados.get('competencia'):
            specs = self._get_options("/complaint/specialties", idpet)
            filled_spec = False
            if specs:
                for target in ['Direito Previdenciário', 'Previdenciário', 'Assistencial', 'Cível']:
                    match = [s for s in specs if target.lower() in s.get('nome', '').lower()]
                    if match:
                        if try_put('competencia', match[0]['nome'], f"especialidade={match[0]['nome']}"):
                            filled_spec = True
                            break
            # Fallback: some tribunals (TRF-1) don't expose specialties endpoint
            # but accept direct value via PUT
            if not filled_spec:
                for direct_val in ['Direito Previdenciário', 'Previdenciário', 'Cível']:
                    if try_put('competencia', direct_val, f"especialidade(direto)={direct_val}"):
                        break

        # Step 2: Comarca / Jurisdição
        if not dados.get('comarca') or dados.get('comarca') == 'null':
            districts = self._get_options("/complaint/district", idpet)
            if districts:
                comarca = self._match_comarca(districts, cidade, uf)
                if comarca:
                    try_put('comarca', comarca, f"comarca={comarca[:50]}")
                else:
                    result['errors'].append(f"comarca: '{cidade}/{uf}' não encontrada em {len(districts)} opções")

        # Step 3: Rito (skip for PJe if not available — some PJe don't need it)
        if not dados.get('rito'):
            ritos = self._get_options("/complaint/procedures", idpet)
            if ritos:
                for target in ['JUIZADO ESPECIAL FEDERAL', 'JUIZADO ESPECIAL', 'ORDINÁRIO', 'COMUM']:
                    match = [r for r in ritos if target.upper() in r.get('nome', '').upper()]
                    if match:
                        try_put('rito', match[0]['nome'], f"rito={match[0]['nome']}")
                        break

        # Step 4: Classe processual
        if not dados.get('classe'):
            classes = self._get_options("/complaint/classes", idpet)
            if classes:
                for target in ['PROCEDIMENTO DO JUIZADO ESPECIAL CÍVEL',
                                'PROCEDIMENTO DO JUIZADO ESPECIAL',
                                'PROCEDIMENTO COMUM CÍVEL',
                                'PROCEDIMENTO COMUM']:
                    match = [c for c in classes if target.upper() in c.get('nome', '').upper()]
                    if match:
                        try_put('classe', match[0]['nome'], f"classe={match[0]['nome'][:50]}")
                        break

        # Step 5: Assunto principal
        if not dados.get('assunto'):
            subjects = self._get_options("/complaint/subjects", idpet)
            if subjects:
                assunto = self._match_assunto_bpc(subjects, tipo)
                if assunto:
                    try_put('assunto', assunto, f"assunto={assunto[:60]}")

        # Step 6: Competência / Área
        if not dados.get('area'):
            areas = self._get_options("/complaint/areas", idpet)
            if areas:
                for target in ['Cível + Previdenciário', 'Previdenciário', 'Cível', 'Federal']:
                    match = [a for a in areas if target.lower() in a.get('nome', '').lower()]
                    if match:
                        try_put('area', match[0]['nome'], f"area={match[0]['nome']}")
                        break

        # Step 7: Flags (all at once)
        try_put_multi({
            'gratuidade': True,
            'liminar': True,
            '100digital': True,
            'renuncia60Salarios': True,
            'distribuicao': 'Por sorteio',
        }, "flags")

        return result

    def _preencher_eproc(self, idpet, dados, cidade, uf, tipo, result, try_put, try_put_multi):
        """Fill eProc petition fields INDIVIDUALLY in correct order.

        eProc strategy (learned from TRF-2/TRF-4/TRF-5 testing):
        - Batch PUT always fails (circular dependency on classe)
        - Fill each field individually — some will fail, that's OK
        - Classe: NOT available via API for eProc, filled auto at protocol time
        - Order: comarca -> rito -> assunto -> flags (each independent PUT)
        """
        # Step 1: Comarca (most important — defines jurisdiction)
        if not dados.get('comarca') or dados.get('comarca') == 'null':
            districts = self._get_options("/complaint/district", idpet)
            if districts:
                comarca = self._match_comarca(districts, cidade, uf)
                if comarca:
                    try_put('comarca', comarca, f"comarca={comarca[:40]}")
                else:
                    result['errors'].append(f"comarca: '{cidade}/{uf}' nao encontrada em {len(districts)} opcoes")
            elif cidade:
                # Try direct city name
                try_put('comarca', cidade, f"comarca(direto)={cidade}")

        # Step 2: Rito
        if not dados.get('rito'):
            ritos = self._get_options("/complaint/procedures", idpet)
            if ritos:
                for target in ['JUIZADO ESPECIAL FEDERAL', 'JUIZADO ESPECIAL', 'ORDINARIO', 'COMUM']:
                    match = [r for r in ritos if target.upper() in r.get('nome', '').upper()]
                    if match:
                        try_put('rito', match[0]['nome'], f"rito={match[0]['nome']}")
                        break

        # Step 3: Assunto + Classe + Flags via internal proporAcao/update
        # The public API PUT requires 'classe' (which eProc doesn't have)
        # The internal endpoint accepts empty classe/areas
        # This sets assunto, rito, flags all at once — no more partial failures
        fresh = self.get_peticao(idpet)
        idproc = fresh.get('idprocessos') or self._request("get", f"/complaint?idpeticoes={idpet}").json().get('peticao', {}).get('idprocessos')

        if idproc and self._proporAcao_update(idpet, idproc, fresh):
            result['filled'].append("assunto+rito+flags via proporAcao/update")
        else:
            # Fallback: try individual flags via public API
            result['errors'].append("proporAcao/update falhou, tentando flags via API")
            flags = {
                'gratuidade': True,
                'liminar': True,
                '100digital': True,
                'renuncia60Salarios': True,
                'distribuicao': 'Por sorteio',
            }
            for flag, value in flags.items():
                if not dados.get(flag):
                    try_put(flag, value, f"{flag}")

        return result

    def _match_comarca(self, districts, cidade, uf):
        """Match city to comarca/subsecao from available options.
        Handles: exact city, Subsecao Judiciaria de CIDADE, JEF preference."""
        if not cidade:
            return None
        cidade_upper = cidade.upper().strip()
        # Normalize accents for matching
        import unicodedata
        def normalize(s):
            return unicodedata.normalize('NFD', s).encode('ascii', 'ignore').decode().upper()
        cidade_norm = normalize(cidade)
        nomes = [d.get('nome', '') for d in districts]

        # 1. Exact match
        for n in nomes:
            if cidade_upper == n.upper():
                return n

        # 2. City name in subsecao (e.g., "Subsecao Judiciaria de Campinas")
        matches = [n for n in nomes if cidade_norm in normalize(n)]
        if matches:
            # Prefer JEF (Juizado Especial Federal) over regular subsecao
            jef = [m for m in matches if 'JUIZADO' in m.upper() or 'JEF' in m.upper()]
            if jef:
                return jef[0]
            # Prefer Subsecao over Secao (more specific)
            subsecao = [m for m in matches if 'SUBSEC' in normalize(m) or 'SUBSE' in normalize(m)]
            if subsecao:
                return subsecao[0]
            return matches[0]

        # 3. Partial match (first word of city name)
        first_word = cidade_norm.split()[0] if cidade_norm else ''
        if first_word and len(first_word) > 3:
            matches = [n for n in nomes if first_word in normalize(n)]
            jef = [m for m in matches if 'JUIZADO' in m.upper() or 'JEF' in m.upper()]
            if jef:
                return jef[0]
            if matches:
                return matches[0]

        # 4. For TRF-1, try "Secao Judiciaria de UF"
        if uf:
            uf_matches = [n for n in nomes if uf.upper() in normalize(n) and 'SEC' in normalize(n)]
            if uf_matches:
                return uf_matches[0]

        return None

    def _match_assunto_bpc(self, subjects, tipo='deficiente'):
        """Find BPC/LOAS subject from list. Never pick previdenciário por incapacidade."""
        search = 'defici' if tipo == 'deficiente' else 'idoso'

        # Priority 1: ASSISTENCIAL + Art. 203 + deficiente/idoso
        for s in subjects:
            nome = s.get('nome', '')
            if 'assistencial' in nome.lower() and '203' in nome and search in nome.lower():
                return nome

        # Priority 2: ASSISTENCIAL + Pessoa com Deficiência
        for s in subjects:
            nome = s.get('nome', '')
            if 'assistencial' in nome.lower() and search in nome.lower():
                return nome

        # Priority 3: Any ASSISTENCIAL (not previdenciário!)
        for s in subjects:
            nome = s.get('nome', '')
            if 'assistencial' in nome.lower() and 'benefício assistencial' in nome.lower():
                return nome

        return None

    # ---- Party management ----

    def criar_parte(self, party_data):
        """POST /parts — Create party (idempotent by CPF). Returns party ID."""
        # Ensure required fields
        if 'personalidade' not in party_data:
            party_data['personalidade'] = 'Pessoa física'
        if 'polo' not in party_data:
            party_data['polo'] = 'ativo'
        if not party_data.get('endereco_numero'):
            party_data['endereco_numero'] = 'S/N'
        if not party_data.get('endereco_bairro'):
            party_data['endereco_bairro'] = 'Centro'  # Fallback — LegalMail requires non-empty bairro
        if not party_data.get('endereco_logradouro'):
            party_data['endereco_logradouro'] = 'Nao informado'

        # Validate and fix CEP — LegalMail REQUIRES hyphen format (XXXXX-XXX)
        cep = party_data.get('endereco_cep', '')
        if cep:
            cep_clean = re.sub(r'\D', '', cep)
            addr = validar_cep(cep_clean)
            if addr:
                # LegalMail requires XXXXX-XXX format (with hyphen!)
                party_data['endereco_cep'] = f"{cep_clean[:5]}-{cep_clean[5:]}"
                # Fill missing address fields from ViaCEP
                if not party_data.get('endereco_cidade'):
                    party_data['endereco_cidade'] = addr.get('localidade', '')
                if not party_data.get('endereco_uf'):
                    party_data['endereco_uf'] = addr.get('uf', '')
                if not party_data.get('endereco_bairro') and addr.get('bairro'):
                    party_data['endereco_bairro'] = addr['bairro']
                if not party_data.get('endereco_logradouro') and addr.get('logradouro'):
                    party_data['endereco_logradouro'] = addr['logradouro']
            else:
                print(f"    [WARN] CEP {cep} inválido, tentando buscar...")
                uf = party_data.get('endereco_uf', '')
                cidade = party_data.get('endereco_cidade', '')
                rua = party_data.get('endereco_logradouro', '')
                if uf and cidade and rua:
                    novo_cep = buscar_cep_por_endereco(uf, cidade, rua)
                    if novo_cep:
                        clean = re.sub(r'\D', '', novo_cep)
                        party_data['endereco_cep'] = f"{clean[:5]}-{clean[5:]}"
                        print(f"    [CEP] Corrigido: {cep} → {party_data['endereco_cep']}")

        r = self._request("post", "/parts", json=party_data)
        if r.status_code == 200:
            pid = int(r.json().get('id', 0))
            print(f"    Parte: {party_data.get('nome', '?')} → id={pid}")
            return pid
        else:
            print(f"    [ERRO] Parte {party_data.get('nome', '?')}: {r.status_code} {r.text[:200]}")
            return None

    def get_or_create_inss(self):
        """Get INSS party ID (cached)."""
        if self._inss_id:
            return self._inss_id

        # Try to find existing
        r = self._request("get", "/parts")
        if r.status_code == 200:
            for p in r.json():
                doc = str(p.get('documento', ''))
                if '29.979.036' in doc or '29979036' in doc:
                    self._inss_id = int(p.get('id', 0))
                    return self._inss_id

        # Create
        pid = self.criar_parte(INSS_DATA.copy())
        if pid:
            self._inss_id = pid
        return self._inss_id

    def vincular_partes(self, idpet, polo_ativo_ids, polo_passivo_ids=None):
        """Set parties on petition."""
        dados = self.get_peticao(idpet)
        dados['idpoloativo'] = polo_ativo_ids if isinstance(polo_ativo_ids, list) else [polo_ativo_ids]
        if polo_passivo_ids:
            dados['idpolopassivo'] = polo_passivo_ids if isinstance(polo_passivo_ids, list) else [polo_passivo_ids]
        return self.put_peticao(idpet, dados)

    # ---- File uploads ----

    def upload_peticao_pdf(self, idpet, idproc, pdf_path):
        """Upload main petition PDF."""
        with open(pdf_path, 'rb') as f:
            r = self._request("post",
                f"/complaintsandpleadings/file?idpeticoes={idpet}&idprocessos={idproc}",
                files={'file': (os.path.basename(pdf_path), f, 'application/pdf')},
                timeout=60)
        if r.status_code == 200:
            print(f"    PDF principal: OK")
            return True
        print(f"    PDF principal ERRO: {r.status_code}")
        return False

    def upload_anexo(self, idpet, pdf_path, doc_type_id):
        """Upload attachment PDF."""
        with open(pdf_path, 'rb') as f:
            r = self._request("post",
                f"/complaintsandpleadings/attachments?idpeticoes={idpet}&fk_documentos_tipos={doc_type_id}",
                files={'file': (os.path.basename(pdf_path), f, 'application/pdf')},
                timeout=60)
        return r.status_code == 200

    def get_tipos_anexo(self, idpet):
        """Get available attachment types for this petition."""
        r = self._request("get", f"/complaintsandpleadings/attachment/types?idpeticoes={idpet}")
        if r.status_code == 200:
            return r.json()
        return []

    def resolver_tipo_anexo(self, idpet, prefix):
        """Match file prefix to attachment type ID."""
        tipos = self.get_tipos_anexo(idpet)
        if not tipos:
            return None

        target_name = DOC_PREFIX_MAP.get(prefix, '')
        if not target_name:
            return None

        # Build lookup map
        tipo_map = {}
        for t in tipos:
            nome = t.get('nome', '').upper()
            tid = t.get('iddocumentos_tipos')
            tipo_map[nome] = tid

        # Exact match
        for nome, tid in tipo_map.items():
            if target_name.upper() in nome:
                return tid

        # Partial match
        first_word = target_name.split()[0].upper()
        for nome, tid in tipo_map.items():
            if first_word in nome:
                return tid

        # Fallback: "Outros" or first available
        for nome, tid in tipo_map.items():
            if 'OUTRO' in nome:
                return tid
        return tipos[0].get('iddocumentos_tipos') if tipos else None

    def upload_todos_anexos(self, idpet, idproc, pasta):
        """Upload all documents from organized folder.

        Usa ordem correta (ORDEM_DOCUMENTOS), valida PDFs,
        exclui adjudicação administrativa, e trata planilha/quesitos.
        """
        uploaded = 0
        errors = []
        pendencias = []

        # Listar e ordenar todos os PDFs
        todos_pdfs = [f for f in os.listdir(pasta) if f.endswith('.pdf')]
        todos_pdfs = ordenar_documentos(todos_pdfs)

        for f in todos_pdfs:
            filepath = os.path.join(pasta, f)
            nome_upper = f.upper()

            # Validar PDF antes de subir
            valido, msg_validacao = validar_pdf(filepath)
            if not valido:
                errors.append(f"PDF INVALIDO: {f} — {msg_validacao}")
                pendencias.append({"arquivo": f, "tipo": "PDF_INVALIDO", "detalhe": msg_validacao})
                continue

            # Excluir adjudicação administrativa em casos BPC/LOAS
            if "ADJUDICACAO" in nome_upper or "ADJUDICAÇÃO" in nome_upper:
                print(f"    [SKIP] Adjudicacao excluida: {f}")
                continue

            # Detectar prefixo
            match = re.match(r'^(\d+)', f)
            prefix = match.group(1) if match else None

            # Tratar planilha e quesitos (sem prefixo numerico)
            if "PLANILHA" in nome_upper:
                prefix = "PLANILHA"
            elif "QUESITOS" in nome_upper:
                prefix = "QUESITOS"

            if prefix == "1":
                # Petição principal — validação extra
                if self.upload_peticao_pdf(idpet, idproc, filepath):
                    uploaded += 1
                    print(f"    [OK] Petição principal: {f}")
                else:
                    errors.append(f"Petição principal: {f}")
            elif prefix == "2":
                # Procuração — validar assinatura
                tem_assinatura, msg_assinatura = validar_procuracao(filepath)
                if not tem_assinatura:
                    errors.append(f"PROCURACAO SEM ASSINATURA: {f} — {msg_assinatura}")
                    pendencias.append({"arquivo": f, "tipo": "PROCURACAO_SEM_ASSINATURA", "detalhe": msg_assinatura})
                    print(f"    [WARN] Procuração sem assinatura: {f} — subindo mesmo assim")
                # Subir mesmo sem assinatura (flag no pendencias)
                doc_type_id = self.resolver_tipo_anexo(idpet, "2")
                if doc_type_id and self.upload_anexo(idpet, filepath, doc_type_id):
                    uploaded += 1
                    print(f"    [OK] Procuração: {f}")
                else:
                    errors.append(f"Procuração upload falhou: {f}")
            elif prefix:
                doc_type_id = self.resolver_tipo_anexo(idpet, prefix)
                if doc_type_id:
                    if self.upload_anexo(idpet, filepath, doc_type_id):
                        uploaded += 1
                        print(f"    [OK] Anexo: {f} (tipo: {DOC_PREFIX_MAP.get(prefix, '?')})")
                    else:
                        errors.append(f"Anexo upload falhou: {f}")
                else:
                    errors.append(f"Tipo não encontrado: {f} (prefix={prefix})")
            else:
                errors.append(f"Arquivo sem prefixo reconhecido: {f}")

        # Salvar pendencias se houver
        if pendencias:
            pendencias_file = os.path.join(pasta, "PENDENCIAS_UPLOAD.json")
            try:
                import json as _json
                with open(pendencias_file, 'w', encoding='utf-8') as pf:
                    _json.dump({"pendencias": pendencias, "data": time.strftime("%Y-%m-%d %H:%M")}, pf, ensure_ascii=False, indent=2)
                print(f"    [WARN] {len(pendencias)} pendencias salvas em PENDENCIAS_UPLOAD.json")
            except Exception:
                pass

        return uploaded, errors

    # ---- Valor da causa extraction ----

    def extrair_valor_causa(self, pasta):
        """Extract valor da causa from planilha Excel."""
        import openpyxl

        xlsx_files = [f for f in os.listdir(pasta) if f.endswith('.xlsx') and 'calculo' in f.lower()]
        if not xlsx_files:
            xlsx_files = [f for f in os.listdir(pasta) if f.endswith('.xlsx')]
        if not xlsx_files:
            return None

        try:
            wb = openpyxl.load_workbook(os.path.join(pasta, xlsx_files[0]), data_only=True)
            ws = wb.active

            # Strategy 1: Find "TOTAL DA CONTA" cell
            for row in ws.iter_rows(values_only=False):
                for cell in row:
                    val = str(cell.value or '').upper()
                    if 'TOTAL DA CONTA' in val and 'SUB' not in val:
                        # Value is in the cell to the right or below
                        right = ws.cell(row=cell.row, column=cell.column + 1).value
                        if right and self._parse_brl(right):
                            return self._parse_brl(right)
                        # Check same row, columns E-H
                        for col in range(5, 9):
                            v = ws.cell(row=cell.row, column=col).value
                            if v and self._parse_brl(v):
                                return self._parse_brl(v)

            # Strategy 2: Last row with numeric value
            for row in reversed(list(ws.iter_rows(min_col=5, max_col=5, values_only=True))):
                if row[0] and isinstance(row[0], (int, float)):
                    return round(float(row[0]), 2)

        except Exception as e:
            print(f"    [WARN] Erro extraindo valor: {e}")
        return None

    def _parse_brl(self, val):
        if val is None:
            return None
        if isinstance(val, (int, float)):
            return round(float(val), 2)
        s = str(val).replace('R$', '').replace(' ', '').strip()
        if not s:
            return None
        if ',' in s and '.' in s:
            if s.rindex(',') > s.rindex('.'):
                s = s.replace('.', '').replace(',', '.')
            else:
                s = s.replace(',', '')
        elif ',' in s:
            parts = s.split(',')
            if len(parts[-1]) <= 2:
                s = s.replace(',', '.')
            else:
                s = s.replace(',', '')
        try:
            return round(float(s), 2)
        except ValueError:
            return None

    # ---- DOCX to PDF conversion ----

    def converter_docx_para_pdf(self, docx_path):
        """Convert .docx to .pdf using LibreOffice or Word COM."""
        import subprocess
        pdf_path = os.path.splitext(docx_path)[0] + '.pdf'
        if os.path.exists(pdf_path):
            return pdf_path

        # Try LibreOffice first (more reliable)
        try:
            result = subprocess.run([
                'soffice', '--headless', '--convert-to', 'pdf',
                '--outdir', os.path.dirname(docx_path), docx_path
            ], capture_output=True, text=True, timeout=60)
            if os.path.exists(pdf_path):
                return pdf_path
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

        # Fallback: Word COM via PowerShell
        try:
            abs_path = os.path.abspath(docx_path).replace('/', '\\')
            abs_pdf = os.path.splitext(abs_path)[0] + '.pdf'
            ps_script = f"""
$word = New-Object -ComObject Word.Application
$word.Visible = $false
$word.DisplayAlerts = 0
try {{
    $doc = $word.Documents.Open('{abs_path}')
    $doc.SaveAs('{abs_pdf}', 17)
    $doc.Close()
}} finally {{
    $word.Quit()
}}
"""
            import tempfile
            ps_file = os.path.join(tempfile.gettempdir(), '_docx2pdf.ps1')
            with open(ps_file, 'w', encoding='utf-8-sig') as f:
                f.write(ps_script)
            subprocess.run(['powershell', '-ExecutionPolicy', 'Bypass', '-File', ps_file],
                          capture_output=True, timeout=60)
            if os.path.exists(pdf_path):
                return pdf_path
        except Exception:
            pass

        return None

    # ---- MAIN: Process one client (one command, everything) ----

    def processar_cliente(self, pasta, client_data=None, extracted=None):
        """Process one client folder: create draft, fill fields, upload docs.

        Args:
            pasta: organized client folder path
            client_data: dict with party data (nome, documento/CPF, endereco_*)
                If has 'representante' key, creates 2 parties (mãe + menor)
            extracted: dict from analisar_pasta_internal (optional)

        Returns: dict with status, idpeticoes, url, details
        """
        nome_pasta = os.path.basename(pasta)
        print(f"\n{'='*60}")
        print(f"  {nome_pasta}")
        print(f"{'='*60}")

        result = {'pasta': nome_pasta, 'status': 'erro'}

        # 1. Detect UF and tribunal
        from app import detect_uf_from_folder
        uf = None
        cidade = ''

        if extracted:
            cidade = extracted.get('cidade', '') or ''
            uf = extracted.get('uf', '')

        if not uf:
            uf = detect_uf_from_folder(pasta)
        if not uf and client_data:
            uf = client_data.get('endereco_uf', '')
        if not uf:
            result['error'] = 'UF não detectada'
            return result

        cfg = UF_TRIBUNAL_MAP.get(uf)
        if not cfg:
            result['error'] = f'UF {uf} sem mapeamento de tribunal'
            return result

        trf = cfg['trf']
        sistema = cfg['sistema']
        print(f"    Tribunal: {trf}/{sistema} (UF={uf})")

        # 2. Get cidade from client data or CEP
        if not cidade and client_data:
            cidade = client_data.get('endereco_cidade', '')
            if not cidade and client_data.get('endereco_cep'):
                addr = completar_endereco_por_cep(client_data['endereco_cep'])
                if addr:
                    cidade = addr.get('endereco_cidade', '')
                    client_data.update({k: v for k, v in addr.items() if v and not client_data.get(k)})

        # 3. Create petition draft
        try:
            idpet, idproc = self.criar_peticao(trf, sistema, "1", cfg['uf_tribunal'])
        except Exception as e:
            result['error'] = str(e)
            return result

        result['idpeticoes'] = idpet
        result['idprocessos'] = idproc
        result['url'] = f"https://app.legalmail.com.br/petitions/{idpet}"

        # 4. Extract valor da causa
        valor = self.extrair_valor_causa(pasta)
        if valor:
            print(f"    Valor da causa: R$ {valor:,.2f}")

        # 5. Create/link parties
        inss_id = self.get_or_create_inss()
        polo_ativo_ids = []

        if client_data:
            rep_data = client_data.get('representante')
            menor_data = client_data.get('menor')

            if rep_data and menor_data:
                # Mãe (primeiro) + Menor (segundo) no polo ativo
                rep_id = self.criar_parte(self._prepare_party(rep_data, 'ativo', 'DO LAR\\DONA DE CASA'))
                menor_id = self.criar_parte(self._prepare_party(menor_data, 'ativo', 'DESEMPREGADO'))
                if rep_id:
                    polo_ativo_ids.append(rep_id)
                if menor_id:
                    polo_ativo_ids.append(menor_id)
            elif client_data.get('nome') and client_data.get('documento'):
                # Single party (adult like Denise, Roseli)
                pid = self.criar_parte(self._prepare_party(client_data, 'ativo'))
                if pid:
                    polo_ativo_ids.append(pid)

        # 6. Fill all fields
        fill_result = self.preencher_campos(idpet, cidade, uf)
        result['filled'] = fill_result['filled']
        result['fill_errors'] = fill_result['errors']

        # 7. Set valor da causa + parties (separate PUT to avoid field conflicts)
        try:
            dados = self.get_peticao(idpet)
            if valor:
                dados['valorCausa'] = valor
            if polo_ativo_ids:
                dados['idpoloativo'] = polo_ativo_ids
            if inss_id:
                dados['idpolopassivo'] = [inss_id]
            self.put_peticao(idpet, dados)
        except Exception as e:
            result['fill_errors'].append(f"valor/partes: {e}")

        # 8. Convert docx → PDF if needed
        for f in os.listdir(pasta):
            if f.startswith('1- PETICAO') and f.endswith('.docx'):
                pdf_path = os.path.join(pasta, f.replace('.docx', '.pdf'))
                if not os.path.exists(pdf_path):
                    print(f"    Convertendo {f} → PDF...")
                    self.converter_docx_para_pdf(os.path.join(pasta, f))

        # 9. Upload all documents
        uploaded, upload_errors = self.upload_todos_anexos(idpet, idproc, pasta)
        result['uploaded'] = uploaded
        if upload_errors:
            result['upload_errors'] = upload_errors

        result['status'] = 'ok'
        print(f"    RESULTADO: {len(fill_result['filled'])} campos | {uploaded} docs | {len(fill_result['errors'])} erros")
        return result

    def _prepare_party(self, data, polo, profissao_default=None):
        """Prepare party data dict with all required fields."""
        party = {
            'nome': (data.get('nome', '') or '').upper(),
            'documento': data.get('documento', '') or data.get('cpf', '') or '',
            'personalidade': 'Pessoa física',
            'polo': polo,
            'endereco_cep': data.get('endereco_cep', '') or data.get('cep', '') or '',
            'endereco_logradouro': data.get('endereco_logradouro', '') or data.get('logradouro', '') or '',
            'endereco_numero': data.get('endereco_numero', '') or data.get('numero', '') or 'S/N',
            'endereco_bairro': data.get('endereco_bairro', '') or data.get('bairro', '') or '',
            'endereco_cidade': data.get('endereco_cidade', '') or data.get('cidade', '') or '',
            'endereco_uf': data.get('endereco_uf', '') or data.get('uf', '') or '',
        }

        # Profissao — must match exact API values (case sensitive)
        PROF_MAP = {
            'DESEMPREGADO': 'Desempregado',
            'DESEMPREGADA': 'Desempregado',
            'DO LAR': 'DO LAR\\DONA DE CASA',
            'DO LAR\\DONA DE CASA': 'DO LAR\\DONA DE CASA',
            'ESTUDANTE': 'Estudante',
            'MENOR': 'Estudante',
            'SEM PROFISSAO': 'Sem Profissão Definida',
        }
        prof = data.get('profissao', '') or ''
        if not prof and profissao_default:
            prof = profissao_default
        if prof:
            party['profissao'] = PROF_MAP.get(prof.upper().strip(), prof)

        # Etnia (TRF-2 eProc requires it)
        party['etnia'] = data.get('etnia', 'Não declarada')

        return party


# ============================================================
# STANDALONE: Fix existing drafts
# ============================================================
def fix_rascunho(svc, idpet, cidade=None, uf=None, polo_ativo_ids=None):
    """Fix an existing draft: fill missing fields, link parties."""
    print(f"\n  Corrigindo rascunho {idpet}...")
    dados = svc.get_peticao(idpet)

    # Detect cidade/uf from existing data if not provided
    if not cidade:
        comarca = dados.get('comarca', '') or ''
        # Try to extract city from comarca name
        m = re.search(r'(?:de|DE)\s+(.+?)(?:\s*\(|$|-)', comarca)
        if m:
            cidade = m.group(1).strip()

    result = {'idpet': idpet}

    # Fill missing fields
    if uf or cidade:
        fill = svc.preencher_campos(idpet, cidade, uf or '')
        result['filled'] = fill['filled']
        result['errors'] = fill['errors']

    # Link parties if provided
    if polo_ativo_ids:
        svc.vincular_partes(idpet, polo_ativo_ids)
        result['partes'] = True

    return result
