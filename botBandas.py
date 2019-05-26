from threading import Thread, Event
import logging
import simplejson
from time import sleep
from datetime import datetime
from utils.webSocket import *
import utils.PMY_REST as pmy
from utils.menu import select_tickers_pase, ask_login_credentials
from strategies.BandasTasa import BandasTasa

def estrategia(ws, ticker_spot="",ticker_futuro="", fecha_vto='',account='',stopping=None):
    est = BandasTasa(ws,q_orders,account=account,ticker_spot=ticker_spot,ticker_futuro=ticker_futuro, fecha_vto=fecha_vto,stopping=stopping)
    est.run()

def run():
    try:
        logger = logging.getLogger(__name__)
        logger.debug("Hello!, please don't kill me")
        stopping = Event()
        user, password, account, entorno,db = ask_login_credentials()
        
        ws = start_ws(stopping,user=user,account=account,entorno=entorno,password=password)
        
        #Subscribe to the OrderReport messages
        subscribeOR(ws,account)

        ticker_spot = "I.RFX20"
        ticker_futuro = "RFX20Jun19"
        fecha_vto = '28-06-2019'
        t_est = Thread(target=estrategia, kwargs={'ws':ws,'ticker_spot':ticker_spot,'ticker_futuro':ticker_futuro, 'fecha_vto':fecha_vto,'account':account, 'stopping':stopping,'db':db},name='BandasTasaStrategy')
        t_est.daemon = True
        t_est.start()

        while True:
            pass
    except KeyboardInterrupt:
        stopping.set()
        t_est.join()
        ws.close()
        logger.debug("You've killed me. I will revenge. Hasta la vista, baby.")

if __name__ == '__main__':
    from utils.createLogger import createLogger
    logger = createLogger()
    run()
