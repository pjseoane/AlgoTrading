import utils.estrategias
import pandas as pd
import numpy as np
import utils.DBtools
import logging


"""
Issues:
    Automate determination of the portfolio cash at init!

"""
class Portfolio():
    """
    Clase para administrar el portfolio.
    Para inicializarla, se necesita conocer la cantidad de cash disponible, y definir una perdida maxima admisible.
    """
    def __init__(self, cash=0., max_loss=50, logger=None):
        self.cash = cash
        self.is_running = False
        self.stock = None
        self.side = None
        self.open_price = 0.
        self.current_price = 0.
        self.size = 0
        self.current_profit = 0.
        self.max_loss = max_loss  #Max loss to risk in a position in cash!
        self.SL_price = 0.
        self.total_profit = 0.
        
        self.logger = logger or logging.getLogger(__name__)

    def compute_portfolio_price(self):
        total = self.cash + self.current_price*self.size
        return total
        
    def update_stock_price(self):
        if self.stock != None:
            self.current_price = DBtools.read_last_price(self.stock)
            self.compute_profit()
        
    def compute_profit(self):
        if self.side == "BUY":
            self.current_profit = self.current_price - self.open_price
        elif self.side == "SELL":
            self.current_profit = self.open_price - self.current_price

    def open_position(self, ticker, price, size, side):
        self.stock = ticker
        self.open_price = price
        self.current_price = price
        self.size = abs(size)
        self.side = side
        self.update_SL()
        logging.info(f'A position in {self.stock} is opened in the {self.side} side at {self.open_price} with {self.size} contract/s. SL level at {self.SL_price}')
        print(f'A position in {self.stock} is opened in the {self.side} side at {self.open_price} with {self.size} contract/s. SL level at {self.SL_price}')
    
    def close_position(self):
        self.compute_profit()
        self.total_profit = self.total_profit + self.current_profit
        print("Closing position!")
        logging.info(f'Profit of the opeartion: {self.current_profit}')
        logging.info(f'Total profit since the bot is running {self.total_profit}')
        logging.info("Closing position")
    
    def stop_loss(self):
        if self.is_running:
            self.update_stock_price()
            # if self.side == "BUY":
            if self.current_price < self.SL_price:
                print('STOP LOSS ALARM!!')
                logging.info("Stop Loss activated!")
                return True
            else:
                print('Actual situation of the strategy: Run forest, run')
                self.update_SL()
                return False
                
    def update_SL(self):
        
        new_SL_price = self.current_price-self.max_loss if self.side == 'BUY' else self.current_price+self.max_loss
        if (new_SL_price > self.SL_price) and (self.side=='BUY'):
            self.SL_price = new_SL_price
            logging.info(f'SL updated to:{self.SL_price}')
        elif (new_SL_price < self.SL_price) and (self.side=='SIDE'):
            self.SL_price = new_SL_price
            logging.info(f'SL updated to:{self.SL_price}')
                
    def check_stoploss(self):
        if self.is_running:
            # print('Side:',self.side,'Open price: ', self.open_price,'Current Price: ', self.current_price,'SL price: ',self.SL_price, 'Current profit:', self.current_profit, 'StopLoss?')
            SL = self.stop_loss()
            if not SL:
                self.update_SL()

if __name__ == '__main__':
    portfolio = Portfolio(1.,1/100)
    portfolio.open_position(ticker="RFX20Mar19",price=44400,size=1, side='BUY')
    portfolio.update_stock_price()
    for i in range (5):
        portfolio.check_stoploss()