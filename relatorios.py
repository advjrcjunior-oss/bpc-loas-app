"""Relatorios ConversApp - Blueprint"""

import os
import json
import time
import requests
import anthropic
from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify, render_template

relatorios_bp = Blueprint("relatorios", __name__)

CONVERSAPP_API_TOKEN = os.environ.get("CONVERSAPP_API_TOKEN", "")
CONVERSAPP_API_BASE = "https://api.wts.chat"
CONVERSAPP_CHANNEL_ID = os.environ.get("CONVERSAPP_CHANNEL_ID", "5395dbba-34f9-42a5-852f-77e5e11a7c94")

# Cache de nomes de atendentes
_agent_names_cache = {}


def _conversapp_get(endpoint, params=None):
    """GET request to ConversApp API."""
    url = f"{CONVERSAPP_API_BASE}{endpoint}"
    headers = {
        "Authorization": f"Bearer {CONVERSAPP_API_TOKEN}",
        "Content-Type": "application/json",
    }
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    return resp


def _get_admin_check():
    from app import _check_admin_token
    return _check_admin_token


def _get_sessions(start_date, end_date, user_id=None, status=None, page=0, page_size=100):
    """Fetch sessions from ConversApp with filters."""
    params = {
        "channelId": CONVERSAPP_CHANNEL_ID,
        "pageSize": page_size,
        "page": page,
        "orderBy": "createdat",
        "orderDirection": "DESCENDING",
    }
    if start_date:
        params["createdAtStart"] = start_date
    if end_date:
        params["createdAtEnd"] = end_date
    if user_id:
        params["userId"] = user_id
    if status:
        params["status"] = status

    resp = _conversapp_get("/chat/v1/session", params=params)
    if resp.status_code == 200:
        return resp.json()
    return {"items": [], "totalItems": 0}


def _get_all_sessions(start_date, end_date, user_id=None):
    """Fetch ALL sessions for a period (paginated)."""
    all_sessions = []
    page = 0
    while True:
        data = _get_sessions(start_date, end_date, user_id=user_id, page=page, page_size=100)
        items = data.get("items", [])
        all_sessions.extend(items)
        if not data.get("hasMorePages", False):
            break
        page += 1
        if page > 50:  # safety limit
            break
    return all_sessions


def _get_contact_name(contact_id):
    """Get contact name by ID."""
    try:
        resp = _conversapp_get(f"/chat/v1/contact/{contact_id}")
        if resp.status_code == 200:
            data = resp.json()
            return data.get("name", "Desconhecido")
    except Exception:
        pass
    return "Desconhecido"


def _parse_time_service(time_str):
    """Parse time string like '2526:20:02' to minutes."""
    if not time_str:
        return 0
    try:
        parts = time_str.split(":")
        hours = int(parts[0])
        minutes = int(parts[1]) if len(parts) > 1 else 0
        return hours * 60 + minutes
    except Exception:
        return 0


def _parse_time_wait(time_str):
    """Parse wait time string to minutes."""
    if not time_str:
        return 0
    try:
        parts = time_str.split(":")
        hours = int(parts[0])
        minutes = int(parts[1]) if len(parts) > 1 else 0
        return hours * 60 + minutes
    except Exception:
        return 0


def _analyze_sessions_with_ai(sessions, period_label):
    """Use Claude Haiku to generate qualitative analysis."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return {"error": "API key nao configurada"}

    # Prepare summary data for AI
    summary_lines = []
    for s in sessions[:100]:  # limit to 100 for token efficiency
        status = s.get("statusDescription", "?")
        origin = s.get("origin", "?")
        wait = s.get("timeWait", "N/A")
        service = s.get("timeService", "N/A")
        last_msg = (s.get("lastMessageText") or "")[:100]
        summary_lines.append(f"Status:{status} | Origem:{origin} | Espera:{wait} | Atendimento:{service} | Ultima msg:{last_msg}")

    summary_text = "\n".join(summary_lines)

    client = anthropic.Anthropic(api_key=api_key)
    resp = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=2000,
        messages=[{
            "role": "user",
            "content": f"""Analise estes dados de atendimento do periodo {period_label} e gere um relatorio executivo profissional.

DADOS ({len(sessions)} sessoes):
{summary_text}

Gere em JSON com esta estrutura:
{{
  "nota_geral": 0-10,
  "resumo_executivo": "2-3 frases sobre o periodo",
  "pontos_positivos": ["lista de pontos fortes"],
  "pontos_melhoria": ["lista de melhorias necessarias"],
  "recomendacoes": ["acoes concretas recomendadas"],
  "tempo_resposta_avaliacao": "avaliacao do tempo de espera",
  "taxa_resolucao_avaliacao": "avaliacao da taxa de conclusao"
}}

Responda APENAS o JSON, sem markdown."""
        }]
    )
    try:
        return json.loads(resp.content[0].text)
    except Exception:
        return {"resumo_executivo": resp.content[0].text}


@relatorios_bp.route("/relatorios")
def relatorios_page():
    """Render reports page."""
    return render_template("relatorios.html")


@relatorios_bp.route("/api/relatorios/resumo", methods=["GET"])
def relatorio_resumo():
    """Get summary report for a period.
    Params: inicio (YYYY-MM-DD), fim (YYYY-MM-DD), atendente (userId, optional)
    """
    if not _get_admin_check()():
        return jsonify({"error": "unauthorized"}), 401
    if not CONVERSAPP_API_TOKEN:
        return jsonify({"error": "CONVERSAPP_API_TOKEN nao configurado"}), 400

    inicio = request.args.get("inicio", "")
    fim = request.args.get("fim", "")

    if not inicio or not fim:
        # Default: last 30 days
        end = datetime.utcnow()
        start = end - timedelta(days=30)
        inicio = start.strftime("%Y-%m-%dT00:00:00Z")
        fim = end.strftime("%Y-%m-%dT23:59:59Z")
    else:
        inicio = f"{inicio}T00:00:00Z"
        fim = f"{fim}T23:59:59Z"

    atendente = request.args.get("atendente", "")

    # Fetch sessions
    sessions = _get_all_sessions(inicio, fim, user_id=atendente or None)

    # Calculate metrics
    total = len(sessions)
    if total == 0:
        return jsonify({"total": 0, "message": "Nenhuma sessao encontrada no periodo"})

    completed = sum(1 for s in sessions if s.get("status") == "COMPLETED")
    in_progress = sum(1 for s in sessions if s.get("status") == "IN_PROGRESS")
    waiting = sum(1 for s in sessions if s.get("status") == "WAITING")

    # Wait times
    wait_times = [_parse_time_wait(s.get("timeWait")) for s in sessions if s.get("timeWait")]
    avg_wait = sum(wait_times) / len(wait_times) if wait_times else 0

    # Origin breakdown
    origin_contato = sum(1 for s in sessions if s.get("origin") == "Contato")
    origin_empresa = sum(1 for s in sessions if s.get("origin") == "Empresa")

    # Agent breakdown
    agents = {}
    for s in sessions:
        uid = s.get("userId", "sem_atendente")
        if uid not in agents:
            agents[uid] = {"total": 0, "completed": 0, "id": uid}
        agents[uid]["total"] += 1
        if s.get("status") == "COMPLETED":
            agents[uid]["completed"] += 1

    # Sessions per day
    daily = {}
    for s in sessions:
        day = s.get("createdAt", "")[:10]
        daily[day] = daily.get(day, 0) + 1

    # Peak hours
    hours = {}
    for s in sessions:
        created = s.get("createdAt", "")
        if len(created) >= 13:
            h = created[11:13]
            hours[h] = hours.get(h, 0) + 1

    result = {
        "periodo": {"inicio": inicio, "fim": fim},
        "total_sessoes": total,
        "status": {
            "concluidos": completed,
            "em_andamento": in_progress,
            "aguardando": waiting,
            "taxa_conclusao": round(completed / total * 100, 1) if total else 0,
        },
        "tempo_espera_medio_min": round(avg_wait, 1),
        "origem": {
            "contato": origin_contato,
            "empresa": origin_empresa,
        },
        "atendentes": list(agents.values()),
        "por_dia": dict(sorted(daily.items())),
        "por_hora": dict(sorted(hours.items())),
    }

    return jsonify(result)


@relatorios_bp.route("/api/relatorios/analise", methods=["GET"])
def relatorio_analise():
    """Get AI-powered qualitative analysis.
    Params: inicio, fim, atendente (optional)
    """
    if not _get_admin_check()():
        return jsonify({"error": "unauthorized"}), 401

    inicio = request.args.get("inicio", "")
    fim = request.args.get("fim", "")

    if not inicio or not fim:
        end = datetime.utcnow()
        start = end - timedelta(days=30)
        inicio = start.strftime("%Y-%m-%dT00:00:00Z")
        fim = end.strftime("%Y-%m-%dT23:59:59Z")
    else:
        inicio = f"{inicio}T00:00:00Z"
        fim = f"{fim}T23:59:59Z"

    atendente = request.args.get("atendente", "")
    period_label = f"{inicio[:10]} a {fim[:10]}"

    sessions = _get_all_sessions(inicio, fim, user_id=atendente or None)
    if not sessions:
        return jsonify({"error": "Nenhuma sessao no periodo"})

    analysis = _analyze_sessions_with_ai(sessions, period_label)
    analysis["total_sessoes_analisadas"] = len(sessions)
    analysis["periodo"] = period_label

    return jsonify(analysis)


@relatorios_bp.route("/api/relatorios/atendentes", methods=["GET"])
def relatorio_atendentes():
    """List agents with session counts."""
    if not _get_admin_check()():
        return jsonify({"error": "unauthorized"}), 401

    # Get recent sessions to discover agents
    data = _get_sessions(None, None, page=0, page_size=100)
    items = data.get("items", [])

    agents = {}
    for s in items:
        uid = s.get("userId")
        if not uid:
            continue
        if uid not in agents:
            agents[uid] = {"id": uid, "sessoes_recentes": 0}
        agents[uid]["sessoes_recentes"] += 1

    # Get total per agent
    for uid in list(agents.keys()):
        total_data = _get_sessions(None, None, user_id=uid, page=0, page_size=1)
        agents[uid]["total_sessoes"] = total_data.get("totalItems", 0)

    return jsonify({"atendentes": list(agents.values())})
