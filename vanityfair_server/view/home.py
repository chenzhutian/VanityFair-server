import sys
from datetime import datetime
from flask import Blueprint, render_template
from flask.views import MethodView

home = Blueprint('home', __name__)

class Home(MethodView):

    def get(self):
        try:
            return render_template('index.html', title="home", year=datetime.now().year)
        except Exception as ex:
            template = "An exception of type {0} occured. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print(sys.exc_info()[0])
            print(message)

home.add_url_rule('/',view_func=Home.as_view("home"))