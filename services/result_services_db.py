import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv
from pymongo import MongoClient
from supabase import create_client
from bson import ObjectId

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set in environment")

MONGO_URL = os.getenv("MONGO_URL") or os.getenv("MONGODB_URI") or os.getenv("MONGO_URI")
if not MONGO_URL:
    raise RuntimeError("MONGO_URL is not set in environment")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def _normalize_email(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    return s.strip().lower()

def _mongo_db():
    client = MongoClient(MONGO_URL)
    return client.get_default_database()

def _best_result_by_job_and_email(rows: List[Dict[str, Any]]) -> Dict[tuple[str, str], Dict[str, Any]]:
    """Pick the most recent test_result for each (jd_id, candidate_email) combination."""
    bucket: Dict[tuple[str, str], Dict[str, Any]] = {}
    for r in rows or []:
        em = _normalize_email(r.get("candidate_email"))
        jd_id = r.get("jd_id")
        if not em or not jd_id:
            continue
        
        key = (str(jd_id), em)
        cur = bucket.get(key)
        
        def ts(x):
            v = x.get("evaluated_at") or x.get("created_at") or x.get("inserted_at")
            try:
                return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
            except Exception:
                return datetime.min
        
        if cur is None or ts(r) > ts(cur):
            bucket[key] = r
    return bucket

def get_all_results(recruiter_id: Optional[str] = None) -> List[Dict[str, Any]]:

    db = _mongo_db()
    jds_col = db["jds"]
    cand_col = db["candidates"]

    jd_filter: Dict[str, Any] = {}
    if recruiter_id:
        try:
            jd_filter["recruiter"] = ObjectId(recruiter_id)
        except Exception:
            return []

    jds = list(jds_col.find(jd_filter, {"_id": 1, "title": 1}))
    if not jds:
        return []

    jd_ids = [d["_id"] for d in jds]
    jd_id_strs = [str(x) for x in jd_ids]
    title_map = {str(d["_id"]): d.get("title") for d in jds}

    question_sets_res = supabase.table("question_sets").select("id, jd_id").in_("jd_id", jd_id_strs).execute()
    question_sets_data = question_sets_res.data or []
    
    qs_to_jd_map = {qs["id"]: qs["jd_id"] for qs in question_sets_data}
    relevant_question_set_ids = list(qs_to_jd_map.keys())

    cand_cursor = cand_col.find({"jdId": {"$in": jd_ids}}, {"_id": 1, "jdId": 1, "name": 1, "email": 1, "score": 1, "testSent": 1})
    cand_docs = list(cand_cursor)

    emails = sorted({e for e in (_normalize_email(c.get("email")) for c in cand_docs) if e})
    supa_rows: List[Dict[str, Any]] = []
    
    if emails and relevant_question_set_ids:
        res = supabase.table("test_results").select("*").in_("candidate_email", emails).in_("question_set_id", relevant_question_set_ids).execute()
        supa_rows = res.data or []

    for row in supa_rows:
        row["jd_id"] = qs_to_jd_map.get(row.get("question_set_id"))

    by_job_and_email = _best_result_by_job_and_email(supa_rows)

    by_jd: Dict[str, List[Dict[str, Any]]] = {}
    for c in cand_docs:
        jd_str = str(c.get("jdId"))
        by_jd.setdefault(jd_str, []).append(c)

    out: List[Dict[str, Any]] = []
    for jd_id_str, cand_list in sorted(by_jd.items()):
        if jd_id_str not in title_map:
            continue

        seen = set()
        unique_cands = []
        for c in cand_list:
            em = _normalize_email(c.get("email"))
            if em and em not in seen:
                seen.add(em)
                unique_cands.append(c)

        total_candidates = len(unique_cands)

        merged: List[Dict[str, Any]] = []
        for c in unique_cands:
            em = _normalize_email(c.get("email"))
            
            lookup_key = (jd_id_str, em)
            sb = by_job_and_email.get(lookup_key, {}) if em else {}

            merged.append({
                "name": sb.get("candidate_name"),
                "email": c.get("email"),
                "testId": sb.get("question_set_id"),
                "score": sb.get("score"),
                "maxScore": sb.get("max_score"),
                "percentage": sb.get("percentage"),
                "status": sb.get("status"),
                "tab switches": sb.get("tab_switches"),
                "text selections": sb.get("text_selections"),
                "copies": sb.get("copies"),
                "pastes": sb.get("pastes"),
                "right clicks": sb.get("right_clicks"),
                "face not visible": sb.get("face_not_visible"),
                "inactivities": sb.get("inactivities"),
            })

        out.append({
            "jobId": jd_id_str,
            "jobTitle": title_map.get(jd_id_str),
            "totalCandidates": total_candidates,
            "candidates": merged
        })

    return out

if __name__ == "__main__":
    import json, sys
    rid = sys.argv[1] if len(sys.argv) > 1 else None
    print(json.dumps(get_all_results(recruiter_id=rid), indent=2, ensure_ascii=False))