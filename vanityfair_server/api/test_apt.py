from flask import Blueprint, jsonify
from flask.views import MethodView

test_api = Blueprint('test-api', __name__, subdomain="api")

class TestAPI(MethodView):
    """description of class"""
    def get(self):
        try:
            return jsonify({"result":"rrerqwer", "arr":["asdf","asdf",1,2]})
        except Exception as ex:
            template = "An exception of type {0} occured. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print(sys.exc_info()[0])
            print(message)

test_api.add_url_rule('/test-api/',view_func=TestAPI.as_view('test-api'))
