from flask import Blueprint, jsonify, request
from services.results_service import fetch_results

results_bp = Blueprint("results", __name__)

@results_bp.route("/result", methods=["GET"])
def get_result():
    # Optional recruiter filter (?recruiterId=<Mongo ObjectId string>)
    recruiter_id = request.args.get("recruiterId")
    data = fetch_results(recruiter_id=recruiter_id)
    return jsonify(data)
