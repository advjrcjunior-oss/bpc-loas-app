from legalmail_service import LegalMailService

class MockLegalMailService(LegalMailService):
    def __init__(self):
        self._options_cache = {}
        self.cert_id = 0
        self.api_key = "dummy"
        self._last_request = 0
        self._session_cookies = None
        self._http = None

    def _request(self, method, endpoint, **kwargs):
        print(f"Called _request: {method} {endpoint}")
        class MockResponse:
            status_code = 200
            def json(self):
                return [{"id": 1, "nome": "Test Type"}]
        return MockResponse()

svc = MockLegalMailService()
print("First call:")
res1 = svc.get_tipos_anexo(123)
print("Result 1:", res1)

print("Second call:")
res2 = svc.get_tipos_anexo(123)
print("Result 2:", res2)
