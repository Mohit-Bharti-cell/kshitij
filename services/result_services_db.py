import os
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
from supabase import create_client
from bson import ObjectId

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set in environment")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

MONGO_URL = os.getenv("MONGO_URL") or os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB") or "recruiter-platform"
if not MONGO_URL:
    raise RuntimeError("MONGO_URL/MONGO_URI not set in environment")

mongo_client = MongoClient(MONGO_URL)
db = mongo_client[MONGO_DB]
candidates_col = db["candidates"]
jds_col = db["jds"]
recruiters_col = db["recruiters"]

def _normalize_email(e: str) -> str:
    return (e or "").strip().lower()

def _latest_result(rows):
    def keyfn(r):
        v = r.get("evaluated_at")
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00")) if v else datetime.min
        except Exception:
            return datetime.min
    return sorted(rows, key=keyfn)[-1] if rows else None

def _build_results_for_jobs(jobs):
    """jobs: list of {'_id': ObjectId, 'title': str}"""
    jd_ids = [str(j['_id']) for j in jobs]
    title_map = {str(j['_id']): j.get('title') for j in jobs}

    obj_ids = [ObjectId(jid) for jid in jd_ids if ObjectId.is_valid(jid)]
    cand_cursor = candidates_col.find({"jdId": {"$in": obj_ids}}, {"_id": 0})
    cand_docs = list(cand_cursor)

    by_jd = {}
    for c in cand_docs:
        jd = c.get("jdId")
        if not jd:
            continue
        by_jd.setdefault(str(jd), []).append(c)

    out = []
    for jd_id_str in sorted(by_jd.keys()):
        cands = by_jd[jd_id_str]

        seen = set()
        uniq = []
        for c in cands:
            em = _normalize_email(c.get("email"))
            if em and em not in seen:
                seen.add(em)
                uniq.append(c)

        total_candidates = len(uniq)

        emails = sorted(seen)
        supa_rows = []
        if emails:
            res = supabase.table("test_results").select("*").in_("candidate_email", emails).execute()
            supa_rows = res.data or []

        bucket = {}
        for r in supa_rows:
            em = _normalize_email(r.get("candidate_email"))
            if not em:
                continue
            bucket.setdefault(em, []).append(r)
        latest = {em: _latest_result(rows) for em, rows in bucket.items()}

        merged = []
        for c in uniq:
            em = _normalize_email(c.get("email"))
            sb = latest.get(em)
            if not sb:
                continue

            mongo_name = c.get("name")
            supa_name = sb.get("candidate_name")
            if mongo_name and str(mongo_name).strip().lower() != "unknown":
                final_name = mongo_name
            else:
                final_name = supa_name or mongo_name or None

            merged.append({
                "candidateId": c.get("candidate_id"),
                "name": final_name,
                "email": c.get("email"),
                "phone": c.get("phone"),
                "testId": sb.get("question_set_id"),
                "score": sb.get("score"),
                "maxScore": sb.get("max_score"),
                "percentage": sb.get("percentage"),
                "status": sb.get("status"),
                "evaluatedAt": sb.get("evaluated_at"),
                "tab swithces": sb.get("tab_switches"),
                "text selections": sb.get("text_selections"),
                "copies": sb.get("copies"),
                "pastes": sb.get("pastes"),
                "right clicks": sb.get("right_clicks"),
                "face not visible": sb.get("face_not_visible"),
                "inactivites": sb.get("inactivities"),
            })

        out.append({
            "jobId": jd_id_str,
            "jobTitle": title_map.get(jd_id_str),
            "totalCandidates": total_candidates,
            "candidates": merged
        })

    return out

def get_all_results():
    jobs = list(jds_col.find({}, {"_id": 1, "title": 1}))
    return _build_results_for_jobs(jobs)

def get_results_for_recruiter(recruiter_email: str):
    if not recruiter_email:
        return []
    rec = recruiters_col.find_one({"email": recruiter_email})
    if not rec:
        return []
    recruiter_id = rec.get("_id")
    jobs = list(jds_col.find({"recruiterId": recruiter_id}, {"_id": 1, "title": 1}))
    if not jobs:
        return []
    return _build_results_for_jobs(jobs)

if __name__ == "__main__":
    import json
    print(json.dumps(get_all_results(), indent=2, ensure_ascii=False))