"""
This script runs the VanityFair_server application using a development server.
"""

from os import environ
from vanityfair_server import app

if __name__ == '__main__':
    HOST = environ.get('SERVER_HOST', 'localhost')
    try:
        PORT = int(environ.get('SERVER_PORT', '5555'))
    except ValueError:
        PORT = 5555
    print(app.config)
    app.config['SERVER_NAME'] = 'localhost:5555'
    app.run(HOST, PORT)
