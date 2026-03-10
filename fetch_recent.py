"""Fetch movements from last 5 days and save as notifications."""
import os, json, time, sys, datetime as dt

# Load env
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ[k.strip()] = v.strip()

import requests

api_key = os.environ.get("LEGALMAIL_API_KEY", "")
BASE = "https://app.legalmail.com.br/api/v1"

CUTOFF_START = "2026-03-05"
CUTOFF_END = "2026-03-10"

def api_get(path):
    r = requests.get(f"{BASE}{path}{'&' if '?' in path else '?'}api_key={api_key}")
    if r.status_code == 429:
        print("  Rate limit, aguardando 60s...", flush=True)
        time.sleep(60)
        r = requests.get(f"{BASE}{path}{'&' if '?' in path else '?'}api_key={api_key}")
    return r

# Step 1: Fetch all processes
print("Buscando processos...", flush=True)
all_procs = []
offset = 0
while True:
    r = api_get(f"/process/all?offset={offset}&limit=50")
    if r.status_code != 200:
        break
    data = r.json()
    if not isinstance(data, list) or len(data) == 0:
        break
    all_procs.extend(data)
    sys.stdout.write(f"\r  {len(all_procs)} processos...")
    sys.stdout.flush()
    if len(data) < 50:
        break
    offset += 50
    time.sleep(2)

active = [p for p in all_procs if p.get("inbox_atual")]
print(f"\nTotal: {len(all_procs)} processos, {len(active)} ativos", flush=True)

# Step 2: Check each active process
all_recent = []
checked = 0

for proc in active:
    idproc = str(proc.get("idprocessos", ""))
    numero = proc.get("numero_processo", "?")
    tribunal = proc.get("tribunal", "?")
    polo_ativo = proc.get("poloativo_nome", "")
    polo_passivo = proc.get("polopassivo_nome", "")
    classe = proc.get("nome_classe", "")
    sistema = proc.get("sistema_tribunal", "")

    time.sleep(2)
    r = api_get(f"/process/autos?idprocesso={idproc}")
    if r.status_code != 200:
        checked += 1
        continue

    autos = r.json()
    if not isinstance(autos, list):
        checked += 1
        continue

    recent = [m for m in autos
              if CUTOFF_START <= (m.get("data_movimentacao", "") or "")[:10] <= CUTOFF_END]

    for m in recent:
        all_recent.append({
            "type": "intimacao",
            "source": "fetch_recent",
            "timestamp": dt.datetime.now().isoformat(),
            "numero_processo": numero,
            "tribunal": tribunal,
            "sistema": sistema,
            "polo_ativo": polo_ativo,
            "polo_passivo": polo_passivo,
            "classe": classe,
            "idprocesso": idproc,
            "idmovimentacoes": str(m.get("idmovimentacoes", "")),
            "titulo_movimentacao": m.get("titulo", ""),
            "data_movimentacao": m.get("data_movimentacao", ""),
            "documentos": [{
                "tipo": "movement",
                "title": m.get("titulo", ""),
                "movement_date": m.get("data_movimentacao", ""),
                "idmovimentacoes": str(m.get("idmovimentacoes", "")),
            }],
            "analyzed": False,
            "analysis": None,
        })

    checked += 1
    if checked % 10 == 0:
        print(f"  Verificados: {checked}/{len(active)}, encontrados: {len(all_recent)} movs", flush=True)

# Sort by date (most recent first)
all_recent.sort(key=lambda x: x.get("data_movimentacao", ""), reverse=True)

print(f"\nTotal: {len(all_recent)} movimentacoes entre {CUTOFF_START} e {CUTOFF_END}", flush=True)

# Save as notifications
data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(data_dir, exist_ok=True)
notif_file = os.path.join(data_dir, "notifications.json")
with open(notif_file, "w", encoding="utf-8") as f:
    json.dump(all_recent, f, ensure_ascii=False, indent=2)

print(f"Salvo em {notif_file}", flush=True)
print(flush=True)

# Print summary
for m in all_recent[:30]:
    print(f'{m["data_movimentacao"][:10]} | {m["numero_processo"]} | {m["tribunal"]} | {m["titulo_movimentacao"][:50]}', flush=True)
if len(all_recent) > 30:
    print(f"... e mais {len(all_recent) - 30}", flush=True)
