from services.result_services_db import get_all_results

def fetch_results(recruiter_id: str | None = None):
    return get_all_results(recruiter_id=recruiter_id)
