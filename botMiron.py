from threading import Thread, Event
import logging
from utils.webSocket import *
from utils.menu import ask_login_credentials

def run():
    try:
        logger = logging.getLogger(__name__)
        logger.debug("Hello!, please don't kill me")
        
        stopping = Event()
        user, password, account, entorno,db = ask_login_credentials()
        
        ws = start_ws(stopping,user=user,account=account,entorno=entorno,password=password)
        
        # Subscribe to the OrderReport messages
        subscribeOR(ws,account)
        
        #Subscribe to MD
        entries = ["LA","BI","OF","SE","OI","TV","IV"]
        tickers = ["RFX20Jun19","RFX20Sep19","I.RFX20","RFXP 06/09 19","DOMay19","DOJun19","DOP 05/06 19"]
        subscribeMD(ws,entries=entries,tickers=tickers)
        
        t = Thread(target=process,kwargs={'stopping':stopping,'db':db},name='ProcessData-Thread')
        t.daemon = True
        t.start()
        
        while True:
            pass
    except KeyboardInterrupt:
        stopping.set()
        ws.close()
        t.join()
        logger.debug("You've killed me. I'll revenge. Hasta la vista, baby")

if __name__ == '__main__':
    from utils.createLogger import createLogger
    logger = createLogger()
    run()