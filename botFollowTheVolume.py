#!/usr/bin/python
# coding: utf-8

from threading import Thread, Event
import logging
from utils.webSocket import *
from utils.menu import select_ticker, ask_login_credentials
from strategies.FollowTheVolume import FollowTheVolume
from threading import Event

def estrategia(ws, ticker="", stopping=Event(),account='',max_loss=50):
    est = FollowTheVolume(ws,q_orders,account=account,ticker=ticker,stopping=stopping, max_loss=max_loss)
    est.run()

def run():
    try:
        logger = logging.getLogger(__name__)
        logger.debug("Hello!, please don't kill me")
        stopping = Event()
        user, password, account, entorno, db = ask_login_credentials()
        ticker, max_loss = select_ticker()
        
        ws = start_ws(stopping,user=user,account=account,entorno=entorno,password=password)
        
        #Subscribe to the OrderReport messages
        subscribeOR(ws,account)

        t_est = Thread(target=estrategia, kwargs={'ws':ws,'ticker':ticker,'stopping':stopping,'max_loss':max_loss,'account':account, 'db':db},name='Estrategia-Thread')
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
