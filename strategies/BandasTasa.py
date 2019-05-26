from strategies.EstrategiaBase import EstrategiaBase
import utils.indicadores
import logging
from datetime import datetime
from threading import Event
from time import sleep

class BandasTasa(EstrategiaBase):
    def __init__(self,ws,q_orders,account='',stopping=Event(),ticker_spot="I.RFX20",ticker_futuro="RFX20Jun19",fecha_vto='2019-06-28',max_loss=150,cash_available=100000, db='remarkets.db'):
        #Datos de los activos
        #Inicializo las variables generales
        super().__init__(ws,q_orders,account,stopping,max_loss=150,cash_available=100000, db=db)
        
        self.logger = logging.getLogger(__name__)
                
        #Agrego las variables especificas de la estrategia
        self.ticker_spot = ticker_spot
        self.ticker_futuro = ticker_futuro
        self.fecha_vto = fecha_vto
        self.spot_LA_price = 0.
        self.futuro_LA_price = 0.
        self.futuro_OF_price = 0.
        self.futuro_BI_price = 0.
        
        self.update_current_prices()
        
        #Espero hasta que se hayan procesado todos los OR para iniciar
        while not self.q_orders.empty():
            pass
        try:
            self.tasa_referencia= float(input('Ingrese la tasa de referencia en %: '))/100.
            self.fs = float(input('Ingrese un factor de seguridad en % (rango % de tasa de referencia donde no operar): '))/100.
        except:
            print('Something goes wrong, and rate of 50% was addopted with 5% of range')
            self.tasa_referencia = 0.5
            self.fs = 5/100
            self.logger.debug('Something goes wrong, and rate of 50% was addopted with 5% of range')

    def signal_maker(self):
        """
        Operador por bandas de tasa implicita, en función de una tasa de referencia. Donde solo me fijo en el ultimo valor de cotizacion y de tasa
        Inputs:
            ticker_spot: Is the name of the spot. For example, I.RFX20
            ticker_futuro: Is the name of the future to analize the implicit rate, For example: RFX20Mar19
            tasa_referencia: Is the reference rate. If the implicit rate is bigger than the reference rate, an short position will be send and viceversa.
            fecha_vto: Day for finishing the future, in string with format %d-%m-%Y: '28-3-2019'
            fs: Safety factor.
        Return:
            position: recomended position size
            side: side for the operatio, sell, buy or hold.
            price: recomended price for the operation.
            
            ISSUES: PRICE IS SETTED AS 5$ more (or less) than the BID side (offer side) for the buy (sell) position.
            5$ because pinto
        """
        try:
            if (self.spot_LA_price != 0. and self.spot_LA_price is not None) and (self.futuro_LA_price != 0. and self.futuro_LA_price is not None):
                # print("Futuro Last Price: ",self.futuro_LA_price,"Spot Last Price: ",self.spot_LA_price)
                tasa = utils.indicadores.tasa_implicita_online(self.spot_LA_price,self.futuro_LA_price,fecha_hoy=datetime.today(),fecha_vto=datetime.strptime(self.fecha_vto,'%d-%m-%Y').replace(hour=23))
                # print('Tasa Actual:',tasa,'Lim Inferior:',self.tasa_referencia*(1-self.fs),'Lim Superior:',self.tasa_referencia*(1+self.fs))
                if tasa is None:
                    return 0,'HOLD',0
                else:
                    if tasa < self.tasa_referencia*(1-self.fs):
                        quantity = 1
                        side = 'BUY'
                        try:
                            price = min(self.futuro_BI_price+5, self.futuro_OF_price-5)
                        except:
                            price = self.futuro_BI_price+5 if self.futuro_OF_price is None else self.futuro_OF_price-5
                    elif tasa > self.tasa_referencia*(1+self.fs):
                        quantity = 1
                        side = 'SELL'
                        try:
                            price = min(self.futuro_BI_price+5, self.futuro_OF_price-5)
                        except:
                            price = self.futuro_BI_price+5 if self.futuro_OF_price is None else self.futuro_OF_price-5
                    else:
                        quantity = 1
                        side = 'HOLD'
                        price = self.futuro_LA_price
                    
                    if self.check_price(side,price,self.futuro_LA_price):
                        return quantity,side,price
                    else:
                        self.logger.debug(f"Error in the price of operation, it's outside limits of operations. Price:{price}, Side:{side}, quantity:{quantity}, LastPrice:{self.futuro_LA_price},OF Price:{self.futuro_OF_price},BI Price:{self.futuro_BI_price}")
                        return 0,"HOLD",0
            else:
                self.logger.debug(f"LAST market information is None. Sleeping for 10 seconds.")
                sleep(10)
                return 0,'HOLD',0
        except KeyboardInterrupt:
            self.logger.debug("Strategy closed by user.")
        except:
            self.logger.exception("Exception performing the estrategia_bandas_tasa signal maker. Sleeping for 15 seconds")
            sleep(15)
            return None, None, None