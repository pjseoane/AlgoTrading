from threading import Thread, Event
import logging
import simplejson
from time import sleep
from datetime import datetime
from utils.webSocket import *
import utils.PMY_REST as pmy
from utils.createLogger import createLogger
from utils.menu import select_tickers_pase, ask_login_credentials
from strategies.ArbitradorPase import ArbitradorPase

def estrategia(ws, pase_ticker="",corto_ticker="",largo_ticker="", account='',stopping=None, db = 'remarkets.db'):
    est = ArbitradorPase(ws, q_orders,account=account, pase_ticker=pase_ticker, corto_ticker=corto_ticker,largo_ticker=largo_ticker, stopping=stopping, comision = 0.18/100, db = db)
    est.run()

def run():
    try:
        logger = logging.getLogger(__name__)
        logger.debug("Hello!, please don't kill me")
        stopping = Event()
        user, password, account, entorno, db = ask_login_credentials()
        
        ws = start_ws(stopping,user=user,account=account,entorno=entorno,password=password)
        
        #Subscribe to the OrderReport messages
        subscribeOR(ws,account)

        ticker_corto,ticker_largo,ticker_pase = select_tickers_pase()
        t_est = Thread(target=estrategia, kwargs={'ws':ws,'pase_ticker':ticker_pase,'corto_ticker':ticker_corto,'largo_ticker':ticker_largo, 'account':account, 'stopping':stopping, 'db':db},name='PaseStrategy')
        t_est.daemon = True
        t_est.start()

        while True:
            pass
    except KeyboardInterrupt:
        stopping.set()
        t_est.join()
        ws.close()
        logger.debug("You've killed me. I'll revenge. Hasta la vista, baby")
        # ws.close()

if __name__ == '__main__':
    logger = createLogger()
    run()
