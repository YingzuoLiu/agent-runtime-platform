import os
import requests
url = os.getenv("RUNTIME_URL", "http://127.0.0.1:8000")
headers = {"Authorization": f"Bearer {os.environ['RUNTIME_API_KEY']}"}
action = {"action_type": "webhook.send", "destination": "demo",
          "idempotency_key": "agent-job-42",
          "input": {"payload": {"text": "hello"}}}
response = requests.post(f"{url}/actions?wait=5", headers=headers, json=action, timeout=7)
response.raise_for_status()
print(response.json())
