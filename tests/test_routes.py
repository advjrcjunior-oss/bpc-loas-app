import pytest

import app as app_module
from app import ADMIN_TOKEN

PROTECTED_POST_ROUTES = [
    "/api/gerar",
    "/api/lote",
    "/api/analisar-pasta",
    "/api/legalmail/rascunho",
    "/api/legalmail/rascunho-lote",
]

MAYAHUB_POST_ROUTES = [
    "/api/mayahub/call",
    "/api/mayahub/campaign",
    "/api/mayahub/campaign/start",
]


@pytest.fixture
def client():
    app_module.app.config["TESTING"] = True
    # Render unhandled errors as 500 responses instead of re-raising, so these
    # tests observe what a real client would receive.
    app_module.app.config["PROPAGATE_EXCEPTIONS"] = False
    with app_module.app.test_client() as c:
        yield c


@pytest.fixture(autouse=True)
def clear_rate_limits():
    app_module._rate_limit_store.clear()
    yield
    app_module._rate_limit_store.clear()


def auth(path):
    return f"{path}?token={ADMIN_TOKEN}"


class TestIndex:
    def test_index_is_public(self, client):
        assert client.get("/").status_code == 200

    def test_index_returns_html(self, client):
        assert "text/html" in client.get("/").content_type


class TestHealth:
    def test_health_requires_admin(self, client):
        assert client.get("/api/health").status_code == 401

    def test_health_ok_with_token(self, client):
        assert client.get(auth("/api/health")).status_code == 200


class TestAdminAuth:
    @pytest.mark.parametrize("path", PROTECTED_POST_ROUTES)
    def test_rejects_missing_token(self, client, path):
        assert client.post(path, json={}).status_code == 401

    @pytest.mark.parametrize("path", PROTECTED_POST_ROUTES)
    def test_rejects_wrong_token(self, client, path):
        assert client.post(f"{path}?token=nope", json={}).status_code == 401

    @pytest.mark.parametrize("path", MAYAHUB_POST_ROUTES)
    def test_mayahub_routes_reject_missing_token(self, client, path):
        assert client.post(path, json={}).status_code == 401

    def test_mayahub_status_rejects_missing_token(self, client):
        assert client.get("/api/mayahub/status").status_code == 401

    def test_relatorios_api_rejects_missing_token(self, client):
        assert client.get("/api/relatorios/resumo").status_code == 401

    def test_unauthorized_body_says_unauthorized(self, client):
        resp = client.post("/api/gerar", json={})
        assert resp.get_json()["error"] == "unauthorized"

    def test_token_accepted_via_header(self, client):
        resp = client.get("/api/health", headers={"X-Admin-Token": ADMIN_TOKEN})
        assert resp.status_code == 200

    def test_empty_token_rejected(self, client):
        assert client.get("/api/health?token=").status_code == 401

    def test_token_prefix_rejected(self, client):
        # Guards against a comparison that accepts prefixes instead of full equality.
        assert client.get(f"/api/health?token={ADMIN_TOKEN[:-1]}").status_code == 401

    def test_token_with_suffix_rejected(self, client):
        assert client.get(f"/api/health?token={ADMIN_TOKEN}x").status_code == 401


class TestRateLimiting:
    def test_api_requests_are_limited_per_ip(self, client):
        # The global limiter allows 60 requests/minute per IP on /api/ paths.
        codes = [client.get(auth("/api/health")).status_code for _ in range(62)]
        assert 429 in codes

    def test_limit_response_mentions_rate_limit(self, client):
        last = None
        for _ in range(62):
            last = client.get(auth("/api/health"))
        assert "rate limit" in last.get_json()["error"]

    def test_non_api_paths_are_not_limited(self, client):
        codes = [client.get("/").status_code for _ in range(62)]
        assert 429 not in codes


class TestMayahubValidation:
    def test_call_requires_phone(self, client, monkeypatch):
        monkeypatch.setattr("mayahub.MAYAHUB_API_KEY", "k")
        monkeypatch.setattr("mayahub.MAYAHUB_ASSISTANT_ID", "1")
        resp = client.post(auth("/api/mayahub/call"), json={})
        assert resp.status_code == 400
        assert "phone" in resp.get_json()["error"]

    def test_call_reports_missing_api_key(self, client, monkeypatch):
        monkeypatch.setattr("mayahub.MAYAHUB_API_KEY", "")
        resp = client.post(auth("/api/mayahub/call"), json={"phone": "11999998888"})
        assert resp.status_code == 400
        assert "MAYAHUB_API_KEY" in resp.get_json()["error"]

    def test_campaign_requires_leads(self, client, monkeypatch):
        monkeypatch.setattr("mayahub.MAYAHUB_API_KEY", "k")
        monkeypatch.setattr("mayahub.MAYAHUB_ASSISTANT_ID", "1")
        resp = client.post(auth("/api/mayahub/campaign"), json={"name": "C"})
        assert resp.status_code == 400
        assert "leads" in resp.get_json()["error"]

    def test_campaign_start_requires_campaign_id(self, client):
        resp = client.post(auth("/api/mayahub/campaign/start"), json={})
        assert resp.status_code == 400
        assert "campaign_id" in resp.get_json()["error"]

    def test_webhook_does_not_require_admin_token(self, client):
        # MayaHub posts here without an admin token, so it must not 401.
        resp = client.post("/api/mayahub/webhook", json={"call_id": "1", "status": "x"})
        assert resp.status_code != 401

    @pytest.mark.xfail(
        reason="app.py defines _get_db only inside `if USE_DB:`, so mayahub."
        "_get_db_funcs() raises ImportError and this public webhook 500s on every "
        "post-call callback whenever DATABASE_URL is unset. Fix: add an else branch "
        "defining a _get_db that raises RuntimeError, which callers already handle.",
        strict=True,
    )
    def test_webhook_returns_ok(self, client):
        resp = client.post("/api/mayahub/webhook", json={"call_id": "1", "status": "x"})
        assert resp.status_code == 200
        assert resp.get_json()["ok"] is True


class TestRelatoriosValidation:
    def test_resumo_reports_missing_token_config(self, client, monkeypatch):
        monkeypatch.setattr("relatorios.CONVERSAPP_API_TOKEN", "")
        resp = client.get(auth("/api/relatorios/resumo"))
        assert resp.status_code == 400
        assert "CONVERSAPP_API_TOKEN" in resp.get_json()["error"]

    def test_relatorios_page_is_public(self, client):
        assert client.get("/relatorios").status_code == 200


class TestLegalmailValidation:
    def test_rascunho_rejects_empty_pasta(self, client):
        resp = client.post(auth("/api/legalmail/rascunho"), json={})
        assert resp.status_code == 400

    def test_rascunho_lote_streams_instead_of_validating_upfront(self, client):
        # SSE endpoint: it opens the stream before checking the payload, so an
        # empty body still yields 200 and reports problems inside the event stream.
        resp = client.post(auth("/api/legalmail/rascunho-lote"), json={})
        assert resp.status_code == 200
        assert "text/event-stream" in resp.content_type


class TestDownloadPathTraversal:
    @pytest.mark.parametrize(
        "attack",
        ["../app.py", "..%2fapp.py", "....//app.py", "/etc/passwd"],
    )
    def test_traversal_does_not_leak_files(self, client, attack):
        resp = client.get(f"/api/download/{attack}")
        assert resp.status_code != 200
