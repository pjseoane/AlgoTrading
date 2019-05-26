from strategies.EstrategiaBase import EstrategiaBase
from strategies import estrategias
import utils.DBtools
from threading import Event
import logging
from datetime import datetime

class FollowTheVolume(EstrategiaBase):
    """
    La idea de esta estrategua es que se plantee una tendencia en funcion del VWAP, y automaticamente o se cierre pq el vwap lo dice, o por un trading proffit.
    """
    
    def __init__(self,ws,q_orders,account,stopping=Event(),ticker='RFX20Jun19',max_loss=50, db='remarkets.db'):
        super().__init__(ws,account,q_orders,stopping, max_loss=max_loss, db=db)
        
        self.ticker_futuro = ticker
        self.ticker_spot= ''
        self.futuro_LA_price = 0.
        self.futuro_OF_price = 0.
        self.futuro_BI_price = 0.
        self.is_running = False
        self.update_current_prices()
        
        self.logger = logging.getLogger(__name__)
        
    def signal_maker(self):
        if not self.stopping.is_set():

            quantity = 1
            try:
                start_date = '2019-04-24'
                start_time = datetime.now()
                df=     estrategias.FollowTheVolume(ticker=self.ticker_futuro,start_date=datetime.today().strftime('%Y-%m-%d'))
                # df=utils.estrategias.FollowTheVolume(ticker=self.ticker_futuro,start_date=start_date)
                if not df.empty:
                    if df['estrategia'].iloc[-1] == 1:
                        side = "BUY"
                    elif df['estrategia'].iloc[-1] == -1:
                        side = "SELL"
                    else:
                        side = "HOLD"
                else:
                    self.logger.warning("The strategy return an empty dataframe. Staying in the HOLD position to prevent errors.")
                    side = "HOLD"
                # self.logger.info(f"The vwap roc choose the {side} position")
            except:
                self.logger.exception("Exception rised when performing the strategy.")
                side = "HOLD"
                return 0,side,0
            
            if side == 'BUY':
                price = self.futuro_BI_price+2 if self.futuro_BI_price != 0 else self.futuro_LA_price
            elif side == 'SELL':
                price = self.futuro_OF_price+2 if self.futuro_OF_price != 0 else self.futuro_LA_price
            else:
                price = 1
            print("Get the side and price takes: ", (datetime.now()-start_time).total_seconds())
            if self.check_price(side,price,self.futuro_LA_price):
                return quantity,side,price
            else:
                self.logger.warning(f"Error in the price of operation, it's outside limits of operations. Price:{price}, Side:{side}, quantity:{quantity}, LastPrice:{self.futuro_LA_price},OF Price:{self.futuro_OF_price},BI Price:{self.futuro_BI_price}")
                return 0,"HOLD",0

        else:
            self.logger.info("The user cancell the strategy")
            return 0,"HOLD",1