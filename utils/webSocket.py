import websocket
import logging
import simplejson
from threading import Event,Thread
from time import sleep
from datetime import datetime, timedelta
from queue import Queue
import numpy as np
import utils.PMY_REST as pmy
import utils.DBtools
from utils.menu import ask_login_credentials

logger = logging.getLogger(__name__)

#General Quee to process the data and save it to the database
q = Queue()
#queue for the strategy class, unique with the order reports 
q_orders = Queue()

def on_message(ws,message):
    try:
        msg = simplejson.loads(message)
        msgType = msg['type'].upper()
        if msgType == 'MD':
            q.put_nowait(msg)
            # # print("Msg sended to the MD queue")
        elif msgType == 'OR':
            fmt = '%Y%m%d-%H:%M:%S.%f-0300'
            transactTime = datetime.strptime(msg['orderReport']['transactTime'],fmt)
            if  transactTime > (datetime.now()-timedelta(minutes=1)):
                q.put_nowait(msg)
                q_orders.put(msg)
            else:
                logger.debug(f'OR not sended to the queue because is an older orderReport.')
            if msg['orderReport']['status']=='REJECTED':
                logger.info(f"The order was rejected: {msg['orderReport']['text']}")
        else:
            logger.debug(f"Error message received: {msg}")
    except:
        msg = simplejson.loads(message)
        if 'status' in msg:
            logger.debug(f"An error message was received: {msg}")
        else:
            logger.exception(f"Exception ocurred in the on_message websocket method. The msg was: {msg}")

def on_error(ws, error):
    logger.error(error)
    ws.close()

def on_close(ws):
    ws.close()
    logger.debug("WS cerrado.")
    pmy.islogin = False

def on_open(ws):
    logger.debug("WS is open!")
    
def createWS():
    """
    Create the websocket with the information (token, WSEndPoint) initialized in the Primary Module.
    """
    if pmy.token != '':
        # websocket.enableTrace(True)
        headers = {'X-Auth-Token:{token}'.format(token=pmy.token)}
        ws = websocket.WebSocketApp(pmy.activeWSEndpoint,
                                         on_message=on_message,
                                         on_error=on_error,
                                         on_close=on_close,
                                         on_open = on_open,
                                         header=headers)
        return ws

def initWS(ws,user="user1", password="password", entorno=1, account='',stopping=Event()):
    """
    Initialize the websocket connection, until a stopping event is activated.
    """
    while not stopping.is_set():
        if not pmy.islogin:
            logger.debug("Logging to get the AUTH-TOKEN again")
            pmy.init(user,password,account,entorno)
            pmy.login()
            if pmy.islogin:
                ws = createWS()
                conn_timeout = 5
                while not ws.sock.connected and conn_timeout:
                    logger.debug('Waiting to establish the websocket connection')
                    # print('Waiting the websocket to connect. Sleeping')
                    sleep(1)
                    conn_timeout -= 1
                logger.debug("Openning WS")
                ws.run_forever(ping_interval=295)
            else:
                logger.error("User not logged in")
                stopping.set()
                ws = None
                
        else:
            logger.debug("Openning WS")
            ws.run_forever(ping_interval=295)
    logger.debug('Websocket clossed due to Stopping Event')
    if ws != None:
        ws.close()
    return ws

def make_MD_msg(ticker,entries):
    #Cretae the message to ask the Market Data of the entries for some ticker.
    msg = simplejson.dumps({'type':"smd","level":1,"entries":entries,"products":[{"symbol":ticker,"marketId":"ROFX"}]})
    return msg

def extract_features(msg):
    """
    take the message received from the API, and transform to a dict.
    """
    start = datetime.now()
    data = {}
    entries =[]
    col = 'orderReport' if msg['type'].upper() == "OR" else 'marketData'
    for entrie, value in msg[col].items():
        entries.append(entrie)
        if isinstance(value, list):
            if len(value)>0:
                value = value[0]
            else:
                value = np.nan
        try:
            for key,value in value.items():
                if key == 'date':
                    value = datetime.fromtimestamp(value/1000)
                
                if value != None or np.isnan(value):
                    try:
                        isnan = np.isnan(value)
                    except:
                        isnan = False
                    if not isnan:
                        data[f'{entrie}_{key}']=value
        except:
            if value != None:
                try:
                    isnan = np.isnan(value)
                except:
                    isnan = False
                if not isnan:
                    data[entrie]=value
    data['date'] = datetime.fromtimestamp(msg['timestamp']/1000)
    return data

def process(stopping=Event(),db='rofex.db'):
    """
    Function to process the messages from the queue, and add to the database.
    """
    try:
        while not stopping.is_set():
            if not q.empty():
                r = q.get()
                # start = time()
                table= "orderReport" if r['type'] == 'or' else utils.DBtools.rename_table(r['instrumentId']['symbol'])
                data = extract_features(r)
                try:
                    utils.DBtools.sql_append(data, table,db=db)
                except:
                    logger.exception("Exception appending data to DataBase.")
                q.task_done()
    except KeyboardInterrupt:
        logger.info("Process Thread end by user")
    except Exception:
        logger.exception("Exception in the process function")

def subscribeOR(ws,account):
    MSG_OSSuscription = simplejson.dumps({"type":"os","account":account,"snapshotOnlyActive":'true'})
    ws.send(MSG_OSSuscription)
    logger.info("Mensaje de subscripcion al OR enviado.")

def subscribeMD(ws,tickers,entries):
    for i,ticker in enumerate(tickers):
        msg = make_MD_msg(ticker,entries)
        ws.send(msg)
        logger.info(f"Mensaje de subscripcion a {ticker} enviado")

def start_ws(stopping=Event(), user='',account='',password='',entorno=''):
    if user == '':
        user, password, account, entorno = ask_login_credentials()
    
    pmy.init(user,password,account,entorno)
    pmy.login()
    ws = createWS()
    t_ws = Thread(target=initWS,kwargs={'ws':ws,'user':user,'password':password,'entorno':entorno, 'account':account,'stopping':stopping},name='WebSocket-Thread')
    t_ws.start()
    try:
        #Wait until the Websocket is open in its thread
        conn_timeout = 5
        sleep(0.5)
        while not ws.sock.connected and conn_timeout:
            logger.debug('Waiting to establish the websocket connection')
            sleep(1)
            conn_timeout -= 1
        return ws
    except:
        ws = None
        logger.error("Websocket could not be oppened.")
        stopping.set()
        return ws