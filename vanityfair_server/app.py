from datetime import datetime
from flask import Flask
from flask import render_template
from vanityfair_server.api.test_apt import test_api
from vanityfair_server.view.home import home

app = Flask(__name__)
app.register_blueprint(test_api)
app.register_blueprint(home)
