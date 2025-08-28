from services.result_services_db import get_all_results, get_results_for_recruiter

def fetch_results(recruiter_email: str | None = None):
    if recruiter_email:
        return get_results_for_recruiter(recruiter_email)
    return get_all_results()
