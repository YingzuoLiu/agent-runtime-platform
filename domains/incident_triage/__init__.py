"""Optional non-Travel reference extension for Phase 7C.

The domain is deliberately synthetic and read-only.  It proves that a trusted
package can register typed input/state, a Planner, an allowlisted tool and final
validation without changing the Runtime manager or HTTP run lifecycle.
"""

from .extension import IncidentTriageExtension
from .models import IncidentTriageInput, IncidentTriageState

__all__ = [
    "IncidentTriageExtension",
    "IncidentTriageInput",
    "IncidentTriageState",
]
