from flask import Flask
from flask_cors import CORS
from results.controller import results_bp

# Create Flask app object (global, not a factory)
app = Flask(__name__)
CORS(app)

# Register your blueprint
app.register_blueprint(results_bp)

# Root route for testing
@app.route("/")
def index():
    return "<h1>Backend OK</h1><p>Use <a href='/result'>/result</a></p>"

# Optional: local testing
if __name__ == "__main__":
    app.run(debug=True, port=5000)
