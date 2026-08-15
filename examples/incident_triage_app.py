"""Executable app composition for the optional Phase 7C reference domain.

Set ``RUNTIME_API_KEYS_JSON`` before importing this module.  The default API
remains unchanged; this process opts in to the trusted incident-triage package
at deployment time.
"""

from api.main import create_app
from domains.incident_triage import IncidentTriageExtension


app = create_app(runtime_extensions=(IncidentTriageExtension(),))
