from strategies.EstrategiaBase import EstrategiaBase
import utils.DBtools
import logging
from threading import Event



class ArbitradorPase(EstrategiaBase):
    def __init__(self,ws,q_orders,account,stopping=Event(),pase_ticker="",corto_ticker="",largo_ticker="",comision=0., db='remarkets.db'):
        super().__init__(ws,account,q_orders,stopping,comision=comision, db=db)
        
        self.logger = logging.getLogger(__name__)
        #Pase de Corto a Largo
        self.pase_ticker = pase_ticker
        self.pase_BI = 0.
        self.pase_BI_size = 0.
        self.pase_OF = 0.
        self.pase_OF_size = 0.
        
        #Futuro Corto
        self.corto_ticker = corto_ticker
        self.corto_BI = 0.
        self.corto_OF = 0.
        self.corto_BI_size = 0.
        self.corto_OF_size = 0.
        
        #Futuro Largo
        self.largo_ticker = largo_ticker
        self.largo_BI = 0.
        self.largo_OF = 0.
        self.largo_BI_size = 0.
        self.largo_OF_size = 0.
        
        self.update_current_prices()

    def signal_maker(self):
        """
        Robado de ARquants!, adaptado a mi codigo
        Arreglar el profit de la operacion!. Puse algo asi nomas sin pensar
        Agregar la comision del broker en las operaciones para ver si es rentable o no.
        """
        if (self.largo_BI*(1-self.comision) - self.corto_OF*(1+self.comision)) > self.pase_OF:
            if (self.largo_BI > 0) and (self.corto_OF > 0) and (self.pase_OF > 0):
                
                # print("Arbitraje detectado, comprando pase, vendiendo combinada")
                size = min(self.largo_BI_size, self.corto_OF_size, self.pase_OF_size)
                size = 1 if size == 0 else size
                self.logger.info(f"Arbitraje detectado en cantidad {size}, comprando pase ({self.pase_OF}), vendiendo combinada: largo ({self.largo_BI}) - corto ({self.corto_OF})")
                # proffit = (self.largo_BI*(1-self.comision) - self.corto_OF*(1+self.comision)) - self.pase_OF*(1+self.comision)
                # self.logger.info(f"Trade Proffit - Broker Comision: {proffit}")
                
                status = ''
                pase_completed = True
                while status != "FILLED":
                    self.place_order(self.pase_OF,"BUY",size,self.pase_ticker)
                    status = self.get_order_status(confirmation_status=['CANCELLED','FILLED','REJECTED'],max_timeout=10)
                    if status == "CANCELLED":
                        pase_completed = False
                        self.logger.info("Hubo que cancelar la orden del pase, no se sigue con la estrategia")
                        break
                    else:
                        self.update_current_prices()
                price_pase = self.pase_OF
                if pase_completed:
                    status = ''
                    while status != "FILLED":
                        self.place_order(self.largo_BI,"SELL",size,self.largo_ticker)
                        status = self.get_order_status(confirmation_status=['CANCELLED','FILLED','REJECTED'],max_timeout=10)

                        if status != "FILLED":
                            self.update_current_prices()
                    price_largo = self.largo_BI

                    status = ''
                    while status != "FILLED":
                        self.place_order(self.corto_OF,"BUY",size,self.corto_ticker)
                        status = self.get_order_status(confirmation_status=['CANCELLED','FILLED','REJECTED'],max_timeout=10)
                        if status != "FILLED":
                            self.update_current_prices()
                    price_corto = self.corto_OF
                    self.trade_profit = (price_largo*(1-self.comision) - price_corto*(1+self.comision)) - price_pase
                    self.total_profit += self.trade_profit
                    self.logger.info(f"The profit of the trade was {self.trade_profit}")
                    self.logger.info(f"The total profit since running is: {self.total_profit}")
                
                #Borro esas variables para que no cancele ordenes que ya estan llenas
                self.property = ''
                self.clOrdId = ''

                
        elif self.largo_OF*(1+self.comision) - self.corto_BI*(1-self.comision) < self.pase_BI:
            if (self.largo_OF > 0) and (self.corto_BI > 0) and (self.pase_BI > 0):
                size = min(self.corto_BI_size, self.largo_OF_size, self.pase_BI_size)
                size = 1 if size == 0 else size
                self.logger.info(f"Arbitraje detectado en cantidad {size}, vendiendo pase ({self.pase_BI}), compra combinada: largo ({self.largo_OF}) - corto ({self.corto_BI})")
                pase_completed = True
                
                status = ''
                while status != "FILLED":
                    self.place_order(self.pase_BI,"SELL",size,self.pase_ticker)
                    status = self.get_order_status(confirmation_status=['CANCELLED','FILLED','REJECTED'],max_timeout=10)
                    if status == "CANCELLED":
                        pase_completed = False
                        self.logger.info("Hubo que cancelar la orden del pase, no se sigue con la estrategia")
                        break
                    else:
                        self.update_current_prices()
                price_pase = self.pase_BI
                if pase_completed:
                    status = ''
                    while status != "FILLED":
                        self.place_order(self.largo_OF,"BUY",size,self.largo_ticker)
                        status = self.get_order_status(confirmation_status=['CANCELLED','FILLED','REJECTED'],max_timeout=10)
                        if status != "FILLED":
                            self.update_current_prices()
                    price_largo = self.largo_OF
                    status = ''
                    while status != "FILLED":
                        self.place_order(self.corto_BI,"SELL",size,self.corto_ticker)
                        status = self.get_order_status(confirmation_status=['CANCELLED','FILLED','REJECTED'],max_timeout=10)
                        if status != "FILLED":
                            self.update_current_prices()
                    price_corto = self.corto_BI
                    
                    self.trade_profit = price_pase - (price_largo*(1-self.comision)-price_corto*(1+self.comision))
                    self.total_profit += self.trade_profit
                    self.logger.info(f"The profit of the trade was {self.trade_profit}")
                    self.logger.info(f"The total profit since running is: {self.total_profit}")
        
        #Borro esas variables para que no cancele ordenes que ya estan llenas
        self.property = ''
        self.clOrdId = ''
        return 0,"HOLD",0
    
    def update_current_prices(self):
        """
        Read the current prices from the database
        The info readed from the DB is a dict with the last, offer and bid data (and size, and other parameters)
        """
        corto_data = utils.DBtools.read_last_row(self.corto_ticker)
        largo_data = utils.DBtools.read_last_row(self.largo_ticker)
        pase_data = utils.DBtools.read_last_row(self.pase_ticker)
        if corto_data is not None:
            if 'BI_price' in corto_data:
                self.corto_BI = corto_data['BI_price'] if corto_data['BI_price'] is not None else self.corto_BI
                self.corto_BI_size = corto_data['BI_size'] if corto_data['BI_size'] is not None else self.corto_BI_size
            if 'OF_price' in corto_data:
                self.corto_OF = corto_data['OF_price'] if corto_data['OF_price'] is not None else self.corto_OF
                self.corto_OF_size = corto_data['OF_size'] if corto_data['OF_size'] is not None else self.corto_OF_size
        if largo_data is not None:
            if 'BI_price' in largo_data:
                self.largo_BI = largo_data['BI_price'] if largo_data['BI_price'] is not None else self.largo_BI
                self.largo_BI_size = largo_data['BI_size'] if largo_data['BI_size'] is not None else self.largo_BI_size
            if 'OF_price' in largo_data:
                self.largo_OF = largo_data['OF_price'] if largo_data['OF_price'] is not None else self.largo_OF
                self.largo_OF_size = largo_data['OF_size'] if largo_data['OF_size'] is not None else self.largo_OF_size
        if pase_data is not None:
            if 'BI_price' in pase_data:
                self.pase_BI = pase_data['BI_price'] if pase_data['BI_price'] is not None else self.pase_BI
                self.pase_BI_size = pase_data['BI_size'] if pase_data['BI_size'] is not None else self.pase_BI_size
            if 'OF_price' in pase_data:
                self.pase_OF = pase_data['OF_price'] if pase_data['OF_price'] is not None else self.pase_OF
                self.pase_OF_size = pase_data['OF_size'] if pase_data['OF_size'] is not None else self.pase_OF_size