"""MayaHub Voice AI Integration - Blueprint"""

import os
import re
import json
import time
import requests
from flask import Blueprint, request, jsonify

mayahub_bp = Blueprint("mayahub", __name__)

MAYAHUB_API_KEY = os.environ.get("MAYAHUB_API_KEY", "")
MAYAHUB_BASE = "https://app.mayahub.ai/api/user"
MAYAHUB_ASSISTANT_ID = os.environ.get("MAYAHUB_ASSISTANT_ID", "")


def _mayahub_request(method, endpoint, **kwargs):
    """Make authenticated request to MayaHub API."""
    url = f"{MAYAHUB_BASE}{endpoint}"
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {MAYAHUB_API_KEY}"
    headers["Accept"] = "application/json"
    return requests.request(method, url, headers=headers, timeout=30, **kwargs)


def _get_admin_check():
    """Import admin check from main app at runtime to avoid circular imports."""
    from app import _check_admin_token
    return _check_admin_token


def _get_db_funcs():
    """Import DB functions from main app at runtime."""
    from app import USE_DB, _db_save, _get_db
    return USE_DB, _db_save, _get_db


@mayahub_bp.route("/api/mayahub/call", methods=["POST"])
def mayahub_make_call():
    """Make a voice call via MayaHub."""
    if not _get_admin_check()():
        return jsonify({"error": "unauthorized"}), 401
    if not MAYAHUB_API_KEY:
        return jsonify({"error": "MAYAHUB_API_KEY nao configurada"}), 400
    if not MAYAHUB_ASSISTANT_ID:
        return jsonify({"error": "MAYAHUB_ASSISTANT_ID nao configurado"}), 400

    data = request.get_json(force=True)
    phone = data.get("phone", "").strip()
    if not phone:
        return jsonify({"error": "phone e obrigatorio"}), 400

    phone_clean = re.sub(r"[^\d+]", "", phone)
    if not phone_clean.startswith("+"):
        phone_clean = "+55" + phone_clean

    variables = {
        "nome_cliente": data.get("nome_cliente", "cliente"),
        "nome_advogado": data.get("nome_advogado", ""),
        "tipo": data.get("tipo", "followup"),
        "detalhes": data.get("detalhes", ""),
        "assunto": data.get("assunto", "seu processo"),
    }

    try:
        resp = _mayahub_request("POST", "/calls", json={
            "phone_number": phone_clean,
            "assistant_id": int(MAYAHUB_ASSISTANT_ID),
            "variables": variables,
        })
        result = resp.json()
        print(f"[MAYAHUB] Ligacao para {phone_clean}: {result}")
        return jsonify(result)
    except Exception as e:
        print(f"[MAYAHUB] Erro ao ligar: {e}")
        return jsonify({"error": str(e)}), 500


@mayahub_bp.route("/api/mayahub/campaign", methods=["POST"])
def mayahub_create_campaign():
    """Create a campaign with multiple leads."""
    if not _get_admin_check()():
        return jsonify({"error": "unauthorized"}), 401
    if not MAYAHUB_API_KEY or not MAYAHUB_ASSISTANT_ID:
        return jsonify({"error": "MayaHub nao configurado"}), 400

    data = request.get_json(force=True)
    campaign_name = data.get("name", f"Campanha {time.strftime('%d/%m %H:%M')}")
    leads = data.get("leads", [])
    if not leads:
        return jsonify({"error": "leads e obrigatorio"}), 400

    try:
        resp = _mayahub_request("POST", "/campaigns", json={
            "name": campaign_name,
            "assistant_id": int(MAYAHUB_ASSISTANT_ID),
        })
        campaign = resp.json()
        campaign_id = campaign.get("data", {}).get("id") or campaign.get("id")
        if not campaign_id:
            return jsonify({"error": "Falha ao criar campanha", "detail": campaign}), 500

        added = 0
        errors = []
        for lead in leads:
            phone = re.sub(r"[^\d+]", "", lead.get("phone", ""))
            if not phone.startswith("+"):
                phone = "+55" + phone
            try:
                lr = _mayahub_request("POST", "/leads", json={
                    "campaign_id": campaign_id,
                    "phone_number": phone,
                    "variables": {
                        "nome_cliente": lead.get("nome_cliente", ""),
                        "nome_advogado": lead.get("nome_advogado", ""),
                        "tipo": lead.get("tipo", "followup"),
                        "detalhes": lead.get("detalhes", ""),
                        "assunto": lead.get("assunto", "seu processo"),
                    }
                })
                if lr.status_code < 300:
                    added += 1
                else:
                    errors.append({"phone": phone, "error": lr.text[:200]})
            except Exception as e:
                errors.append({"phone": phone, "error": str(e)})

        print(f"[MAYAHUB] Campanha {campaign_id}: {added} leads, {len(errors)} erros")
        return jsonify({
            "campaign_id": campaign_id,
            "name": campaign_name,
            "leads_added": added,
            "errors": errors,
        })
    except Exception as e:
        print(f"[MAYAHUB] Erro ao criar campanha: {e}")
        return jsonify({"error": str(e)}), 500


@mayahub_bp.route("/api/mayahub/campaign/start", methods=["POST"])
def mayahub_start_campaign():
    """Start a campaign."""
    if not _get_admin_check()():
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(force=True)
    cid = data.get("campaign_id")
    if not cid:
        return jsonify({"error": "campaign_id obrigatorio"}), 400
    try:
        resp = _mayahub_request("PUT", "/campaigns/status", json={
            "campaign_id": int(cid),
            "status": "active",
        })
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@mayahub_bp.route("/api/mayahub/status", methods=["GET"])
def mayahub_status():
    """Get MayaHub integration status."""
    if not _get_admin_check()():
        return jsonify({"error": "unauthorized"}), 401
    result = {
        "configured": bool(MAYAHUB_API_KEY and MAYAHUB_ASSISTANT_ID),
        "assistant_id": MAYAHUB_ASSISTANT_ID,
        "has_api_key": bool(MAYAHUB_API_KEY),
    }
    if MAYAHUB_API_KEY:
        try:
            resp = _mayahub_request("GET", "/assistants/get")
            assistants = resp.json().get("data", [])
            result["assistants"] = [{"id": a["id"], "name": a["name"]} for a in assistants]
            resp2 = _mayahub_request("GET", "/phone-numbers")
            phones = resp2.json() if resp2.status_code == 200 else []
            result["phone_numbers"] = phones
        except Exception as e:
            result["error"] = str(e)
    return jsonify(result)


@mayahub_bp.route("/api/mayahub/webhook", methods=["POST"])
def mayahub_webhook():
    """Receive post-call webhook from MayaHub."""
    data = request.get_json(force=True)
    call_id = data.get("call_id") or data.get("id", "?")
    status = data.get("status", "unknown")
    duration = data.get("duration", 0)
    phone = data.get("phone_number", "?")
    transcript = data.get("transcript", "")
    recording_url = data.get("recording_url", "")
    variables = data.get("variables", {})

    print(f"[MAYAHUB WEBHOOK] Call {call_id}: {status} | {phone} | {duration}s")

    USE_DB, _db_save, _ = _get_db_funcs()
    if USE_DB:
        try:
            call_data = {
                "call_id": call_id,
                "phone": phone,
                "status": status,
                "duration": duration,
                "transcript": transcript,
                "recording_url": recording_url,
                "variables": variables,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            _db_save(f"mayahub_call_{call_id}", call_data)
            print(f"[MAYAHUB WEBHOOK] Call {call_id} salva no DB")
        except Exception as e:
            print(f"[MAYAHUB WEBHOOK] Erro ao salvar: {e}")

    return jsonify({"ok": True})


@mayahub_bp.route("/api/mayahub/calls", methods=["GET"])
def mayahub_list_calls():
    """List recent MayaHub calls from DB."""
    if not _get_admin_check()():
        return jsonify({"error": "unauthorized"}), 401
    USE_DB, _, _get_db = _get_db_funcs()
    if not USE_DB:
        return jsonify({"error": "DB nao disponivel"}), 400
    try:
        conn = _get_db()
        cur = conn.cursor()
        cur.execute("SELECT key, value FROM kv_store WHERE key LIKE 'mayahub_call_%%' ORDER BY key DESC LIMIT 50")
        rows = cur.fetchall()
        conn.close()
        calls = []
        for key, val in rows:
            try:
                calls.append(json.loads(val))
            except Exception:
                pass
        return jsonify({"calls": calls, "total": len(calls)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
