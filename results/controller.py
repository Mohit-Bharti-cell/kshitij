from flask import Blueprint, jsonify, request
from services.results_service import fetch_results
from services.result_services_db import get_candidate_all_results

results_bp = Blueprint("results", __name__)

@results_bp.route("/result", methods=["GET"])
def get_result():
    recruiter_id = request.args.get("recruiterId")
    data = fetch_results(recruiter_id=recruiter_id)
    return jsonify(data)


@results_bp.route("/candidate_results")
def candidate_results():
    email = request.args.get("email")
    if not email:
        return jsonify({"error": "Missing email"}), 400
    results = get_candidate_all_results(email)
    if not results:
        return jsonify([]), 200
    return jsonify(results)
