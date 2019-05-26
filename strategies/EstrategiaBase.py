import logging
from threading import Event
import pandas as pd
import simplejson
from time import sleep
from datetime import datetime
import utils.DBtools

class EstrategiaBase():
    def __init__(self, ws, q_orders,account='', stopping=Event(), comision = 0.109/100, cash_available=100000, max_loss = 150,db='remarkets.db'):
        #Websocket and account data.
        self.account = account
        self.ws = ws
        self.db = db
        
        #Porfolio data.
        # A unified new class is better option to handle this variables.
        self.cash_available = cash_available
        self.trade_profit = 0.
        self.total_profit = 0.
        self.comision = comision
        
        #Thread stopping event
        self.stopping = stopping
        
        #Datos de la estrategia que estamos ejecutando
        self.is_running = False
        self.open_price = 0.
        self.trailing_stop = 0.
        self.quantity = 0
        self.side = ''
        self.max_loss = max_loss
        
        #Datos de la orden que estamos ejecutando.
        self.q_orders = q_orders
        self.order_status = ''  #Current order_status
        self.clOrdId = ''   #Current orderID
        self.property = ''  #Current property of the order

    def create_variables(self,variables):
        pass

    def update_current_prices(self):
        """
        
        Read the current prices from the database
        The info readed from the DB is a dict with the last, offer and bid data (and size, and other parameters)
        """
        if self.ticker_spot != '':
            spot_data = utils.DBtools.read_last_row(self.ticker_spot,db=self.db)
            if spot_data is not None:
                if 'LA_price' in spot_data:
                    self.spot_LA_price = spot_data['LA_price'] if spot_data['LA_price'] is not None else self.spot_LA_price
        if self.ticker_futuro != '':
            futuro_data = utils.DBtools.read_last_row(self.ticker_futuro,db=self.db)
            if futuro_data is not None:
                if 'LA_price' in futuro_data:
                    self.futuro_LA_price = futuro_data['LA_price'] if futuro_data['LA_price'] is not None else self.futuro_LA_price
                if 'BI_price' in futuro_data:
                    self.futuro_BI_price = futuro_data['BI_price'] if futuro_data['BI_price'] is not None else self.futuro_BI_price
                if 'OF_price' in futuro_data:
                    self.futuro_OF_price = futuro_data['OF_price'] if futuro_data['OF_price'] is not None else self.futuro_OF_price

    def get_order_status(self, max_timeout=30,confirmation_status = ['FILLED','REJECTED']):
        init_time = datetime.now()
        timeout = max_timeout
        order_status = ''
        #Lo transformo a lista por si envio un string unico.
        confirmation_status = confirmation_status if isinstance(confirmation_status, list) else [confirmation_status]
        while order_status not in confirmation_status:
            if not self.q_orders.empty():
                data = self.q_orders.get()
                or_msg = data['orderReport']
                # if 'origclOrdId' in or_msg:
                    # origclOrdId = or_msg['clOrdId']
                order_status = or_msg['status']
                self.clOrdId = or_msg['clOrdId']
                self.property = or_msg['proprietary']
                self.logger.info(f"Order status: {order_status}")
                # print(f"Order Status: {order_status}")
            elif 'PENDING' in order_status:
                pass
            else:
                timeout = max_timeout - (datetime.now()-init_time).seconds
                if timeout<=0:
                    self.logger.info("TIMEOUT waiting to fill the order. I'm Cancelling the order")
                    print("TIMEOUT!, I'm cancelling the order")
                    order_status = self.cancel_order()
                    print(f"Order Status: {order_status}")
                    return order_status
                else:
                    # if timeout<=3:
                        # print("Timeout to cancell the order:", timeout, "seconds")
                    pass
        return order_status

    def make_order_msg(self,ticker,price,quantity,side):
        sendOrder = {"type":"no",
                     "product":{"symbol":ticker,
                                "marketId":"ROFX"},
                     "price":str(price),
                     "quantity":str(quantity),
                     "side":side,
                     "account":self.account}
        return simplejson.dumps(sendOrder)

    def place_order(self,price,side,quantity,ticker=''):
        ticker = self.ticker_futuro if ticker == '' else ticker
        
        msg = self.make_order_msg(ticker,price,quantity,side)
        self.ws.send(msg)
        self.logger.info(f"An order of {side} was sent for {quantity} of {ticker} at price {price}")

    def cancel_order(self):
        cancelMsg = {"type":"co",
            "clientId":self.clOrdId,
            "proprietary":self.property}
        msg = simplejson.dumps(cancelMsg)
        self.ws.send(msg)
        self.logger.info(f"Sending message to cancell the order {self.clOrdId} of property {self.property}")
        or_status = ''
        #Tuve que agregar que el queue este vacio, pq me llegan varios mensajes de cancelado, no se pq. Si el cancell es via matriz llega uno solo.
        while (or_status != 'CANCELLED') or not self.q_orders.empty():
            or_status = self.get_order_status(confirmation_status = "CANCELLED")
        self.logger.info(f"The order {self.clOrdId} was cancelled")
        return or_status

    def get_opossite_side(self):
        if self.side == 'BUY':
            return 'SELL'
        elif self.side == 'SELL':
            return 'BUY'

    def close_position(self, timeout = 20):
        """
        method to close the current position.
        We need to specify the timeout in seconds, that we are aceptting to wait until the order is filled.
        If the timeout is small, we are more agressive, to close the current position.
        """
        if self.is_running:
            #Busco el lado opuesto al que tengo abierto
            new_side = self.get_opossite_side()
            self.update_current_prices()
            price = self.futuro_OF_price if new_side == 'BUY' else self.futuro_BI_price
            self.place_order(price,new_side,self.quantity)
            or_status = ''
            while or_status not in ["CANCELLED","FILLED"]:
                or_status = self.get_order_status(max_timeout=10)
                if or_status == 'CANCELLED':
                    self.update_current_prices()
                    price = self.futuro_OF_price if new_side == 'BUY' else self.futuro_BI_price
                    self.place_order(price,new_side,self.quantity)

            self.update_profit()
            self.logger.info(f"{self.quantity} {self.side} Position/s was closed at price {self.futuro_LA_price}")
            self.logger.info(f"The profit of the trade was {self.trade_profit}")
            self.is_running = False
            self.trailing_stop = 0.
            self.open_price = 0.
            self.side = ''
            self.quantity = 0.
            self.total_profit = self.total_profit + self.trade_profit
            self.logger.info(f"The profit since i'm running is: {self.total_profit}")
            print(f"The profit since i'm running is: {self.total_profit}")
            self.trade_profit = 0.
            self.property = ''
            self.clOrdId = ''
        else:
            print("There is no position to close")
            self.logger.info("There is no position to close")

    def check_SL(self):
        if self.side == 'BUY':
            if self.futuro_LA_price < self.trailing_stop:
                self.logger.info(f'Trailling stop activated in the {self.side} position at price {self.futuro_LA_price}')
                self.close_position()
                print("Trailling Stop activated!")
        elif self.side == 'SELL':
            if self.futuro_LA_price > self.trailing_stop:
                self.logger.info(f'Trailling stop activated in the {self.side} position at price {self.futuro_LA_price}')
                self.close_position()
                print("Trailling Stop activated!")

    def update_trailling_stop(self):
        if self.trailing_stop == 0:
            self.trailing_stop = self.open_price - self.max_loss if self.side == "BUY" else self.open_price + self.max_loss
            self.logger.info(f'Trailling Stop Updated at level {self.trailing_stop}')
            print(f'Trailling Stop Updated at level {self.trailing_stop}')
        else:
            if self.side == 'BUY':
                if self.futuro_LA_price-self.max_loss > self.trailing_stop:
                    self.trailing_stop = self.futuro_LA_price - self.max_loss
                    self.logger.info(f'Trailling Stop Updated at level {self.trailing_stop}')
                    print(f'Trailling Stop Updated at level {self.trailing_stop}')
            elif self.side == 'SELL':
                if self.futuro_LA_price+self.max_loss < self.trailing_stop:
                    self.trailing_stop = self.futuro_LA_price + self.max_loss
                    self.logger.info(f'Trailling Stop Updated at level {self.trailing_stop}')
                    print(f'Trailling Stop Updated at level {self.trailing_stop}')

    def update_profit(self):
        if self.side == 'BUY':
            self.trade_profit = self.futuro_LA_price - self.open_price
        elif self.side == 'SELL':
            self.trade_profit = self.open_price - self.futuro_LA_price
        print(f"Current proffit: {self.trade_profit}")

    def signal_maker(self):
        """
        Se debe generar una nueva clase con herencia de esta estrategia base. Y se debe agregar el metodo signal_maker, que genere las variables:
        quantity: integer
        side: string ("BUY", "HOLD" or "SELL")
        price: float
        """
        quantity = 0
        side = "HOLD"
        price = 0.
        return quantity, side, price

    def stop_strategy(self):
        if self.property != '':
            self.cancel_order()
        if self.quantity > 0:
            self.close_position()
        self.is_running = False

    def check_price(self,side,price_operation,operational_factor=0.1):
        if side == "BUY":
            return price_operation>self.futuro_LA_price*(1-operational_factor)
        else:
            return price_operation<self.futuro_LA_price*(1+operational_factor)
        
    def position_manager(self,price,side,quantity):
        if self.is_running:
            if (side == 'HOLD') or (side == self.side):
                self.update_profit()
                self.update_trailling_stop()
                self.check_SL()
        if side != self.side and self.is_running:
            # print(f'INFO - The strategy said that we have to change the {self.side} position to {side}')
            # self.logger.info(f'The strategy said that we have to change the {self.side} position to {side}')
            if side != 'HOLD':
                self.close_position()
        if not self.is_running:
            if side != 'HOLD':
                try:
                    pass_check = self.check_price(side,price)
                except:
                    pass_check = True
                    self.logger.exception("The bot is taking position without checking the price, due to an error in the check_price function")
                
                if pass_check:
                    print(f"INFO - I'm opening a new position of {side} at price {price}.")
                    self.place_order(price,side,quantity)
                    self.order_status = self.get_order_status(max_timeout=60)
                    if self.order_status == 'FILLED':
                        print("Starting the Strategy!")
                        self.open_price = price
                        self.quantity = quantity
                        self.side = side
                        self.update_trailling_stop()
                        self.is_running = True
                        self.property = ''
                        self.clOrdId = ''
                    elif self.order_status == 'REJECTED':
                        self.property = ''
                        self.clOrdId = ''
                else:
                    self.logger.info("The price adopted did not pasas the check.")

    def run(self):
        #Initialize the strategy
        self.logger.debug(f"Inicializando estrategia {self.__class__.__name__}")
        while not self.stopping.is_set():
            try:
                if self.ws.sock.connected:
                    #Get strategy values
                    self.update_current_prices()

                    #Take strategy quantity, side and price.
                    quantity, side, price = self.signal_maker()
                    
                    #Agrego, mantengo o cierro posicion?
                    self.position_manager(price,side,quantity)
                    
                    #Duermo por 1 segundo, solo para testing
                    # sleep(1)
            except:
                self.logger.exception("Exception running the strategy! Sleeping for 5 seconds, if this error continue, please close the BOT..")
                sleep(5)

        self.logger.debug("Estrategia was stopped by user. I'll be close all my opened positions.")
        self.stop_strategy()
        