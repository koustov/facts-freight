from flask import Flask, json
from initializer import initialize

companies = [{"id": 1, "name": "Company One"}, {"id": 2, "name": "Company Two"}]

api = Flask(__name__)
initialize()


@api.route("/companies", methods=["GET"])
def get_companies():
    return json.dumps(companies)


if __name__ == "__main__":
    api.run()
