from flask import Flask
from flask_cors import CORS
from results.controller import results_bp

app = Flask(__name__)
CORS(app)

app.register_blueprint(results_bp)

@app.route("/")
def index():
    return "<h1>Backend OK</h1><p>Use <a href='/result'>/result</a></p>"

if __name__ == "__main__":
    app.run(debug=True, port=5000)
