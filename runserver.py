"""
This script runs the VanityFair_server application using a development server.
"""

from os import environ
from DataCenter import BillBoard as bb
from VanityFairServer import app

if __name__ == '__main__':
    print(bb.GetBillBoardOn("2016-03-31"))
    
    #HOST = environ.get('SERVER_HOST', 'localhost')
    #try:
    #    PORT = int(environ.get('SERVER_PORT', '5555'))
    #except ValueError:
    #    PORT = 5555
    #app.run(HOST, PORT)

