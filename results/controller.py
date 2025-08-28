from flask import Blueprint, jsonify, request
from services.results_service import fetch_results

results_bp = Blueprint("results", __name__)

@results_bp.route("/result", methods=["GET"])
def get_result():
    recruiter_email = request.args.get("recruiter_email", type=str)
    data = fetch_results(recruiter_email)
    return jsonify(data)
