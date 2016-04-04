"""
This script runs the VanityFair_server application using a development server.
"""

from os import environ
from DataCenter import BillBoard as bb
from DataCenter import Crawler as cr

if __name__ == '__main__':
    print("dd")
    cr.GetBillBoardDataFromEM("2015-11-10")
    #print(cr.GetBillBoardDetailDataFromEM("2015-11-10","300359"))
    #HOST = environ.get('SERVER_HOST', 'localhost')
    #try:
    #    PORT = int(environ.get('SERVER_PORT', '5555'))
    #except ValueError:
    #    PORT = 5555
    #app.run(HOST, PORT)

