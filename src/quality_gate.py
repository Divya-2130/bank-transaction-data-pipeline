"""Critical row-count and reject-rate gate."""
from .errors import QualityGateError


def quality_gate(state: dict) -> dict:
    counts = state["counts"]
    total = counts["total_rows"]
    clean = counts["clean_rows"]
    rejected = counts["rejected_count"]
    if total != clean + rejected:
        raise QualityGateError(
            f"Row counts do not add up: input={total}, clean={clean}, rejected={rejected}"
        )
    reject_rate = (rejected / total) if total else 0.0
    state["counts"]["reject_rate"] = reject_rate
    limit = state["config"].max_reject_rate
    if reject_rate > limit:
        raise QualityGateError(
            f"Reject rate {reject_rate:.2%} is above configured limit {limit:.2%}"
        )
    return {"row_count": total, "rejected_count": rejected}
