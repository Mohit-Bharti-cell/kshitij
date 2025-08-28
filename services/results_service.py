from services.result_services_db import get_all_results

def fetch_results(recruiter_id: str | None = None):
    """Fetch results, optionally filtered by recruiter Mongo _id (as string)."""
    return get_all_results(recruiter_id=recruiter_id)
