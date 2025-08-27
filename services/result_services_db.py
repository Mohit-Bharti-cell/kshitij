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

def get_all_results():
    all_cands = list(candidates_col.find({}, {"_id": 0}))
    by_jd = {}
    for c in all_cands:
        jd_id = c.get("jdId")
        if not jd_id:
            continue
        by_jd.setdefault(str(jd_id), []).append(c)

    jd_obj_ids = []
    for s in by_jd.keys():
        try:
            jd_obj_ids.append(ObjectId(s))
        except Exception:
            pass

    title_map = {}
    if jd_obj_ids:
        for d in jds_col.find({"_id": {"$in": jd_obj_ids}}, {"_id": 1, "title": 1}):
            title_map[str(d["_id"])] = d.get("title")

    out = []
    for jd_id_str in sorted(by_jd.keys()):
        cand_docs = by_jd[jd_id_str]

        seen = set()
        unique_cands = []
        for c in cand_docs:
            em = _normalize_email(c.get("email"))
            if em and em not in seen:
                seen.add(em)
                unique_cands.append(c)

        total_candidates = len(unique_cands)

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
        for c in unique_cands:
            em = _normalize_email(c.get("email"))
            sb = latest.get(em)
            if not sb:
                continue
            mongo_name = c.get("name")
            supa_name = sb.get("candidate_name")
            if mongo_name and str(mongo_name).strip() and str(mongo_name).strip().lower() != "unknown":
                name_final = mongo_name
            else:
                name_final = supa_name or mongo_name or None

            merged.append({
                "candidateId": c.get("candidate_id"),
                "name": name_final,
                "email": c.get("email"),
                "phone": c.get("phone"),
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
                "inactivites": sb.get("inactivities"),
            })

        out.append({
            "jobId": jd_id_str,
            "jobTitle": title_map.get(jd_id_str),
            "totalCandidates": total_candidates,
            "candidates": merged
        })

    return out

if __name__ == "__main__":
    import json
    print(json.dumps(get_all_results(), indent=2, ensure_ascii=False))
