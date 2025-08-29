
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

def _best_result_by_email(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Pick the most recent test_result per candidate_email."""
    bucket: Dict[str, Dict[str, Any]] = {}
    for r in rows or []:
        em = _normalize_email(r.get("candidate_email"))
        if not em:
            continue
        cur = bucket.get(em)
        def ts(x):
            v = x.get("evaluated_at") or x.get("created_at") or x.get("inserted_at")
            try:
                return datetime.fromisoformat(str(v).replace("Z","+00:00"))
            except Exception:
                return datetime.min
        if cur is None or ts(r) > ts(cur):
            bucket[em] = r
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

    cand_cursor = cand_col.find({"jdId": {"$in": jd_ids}}, {"_id": 1, "jdId": 1, "name": 1, "email": 1, "score": 1, "testSent": 1})
    cand_docs = list(cand_cursor)

    emails = sorted({e for e in (_normalize_email(c.get("email")) for c in cand_docs) if e})
    supa_rows: List[Dict[str, Any]] = []
    if emails:
        res = supabase.table("test_results").select("*").in_("candidate_email", emails).execute()
        supa_rows = res.data or []

    by_email = _best_result_by_email(supa_rows)

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
            sb = by_email.get(em, {}) if em else {}

            merged.append({
                "name": c.get("name"),
                "email": c.get("email"),
                "testId": sb.get("question_set_id"),
                "score": sb.get("score"),
                "maxScore": sb.get("max_score"),
                "percentage": sb.get("percentage"),
                "status": sb.get("status"),
                "tab_switches": sb.get("tab_switches"),
                "text_selections": sb.get("text_selections"),
                "copies": sb.get("copies"),
                "pastes": sb.get("pastes"),
                "right_clicks": sb.get("right_clicks"),
                "face_not_visible": sb.get("face_not_visible"),
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
