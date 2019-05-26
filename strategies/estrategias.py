import pandas as pd
import numpy as np
import utils.DBtools
from datetime import datetime
import logging
from utils.finance_tools import getOHLC
import utils.indicadores

"""
issues:
    * Importantes:
        - Implementacion de factor de seguridad en el cruce de medias
        - Implementar más estrategias, de acorde a la operatoria buscada.
        - Analizar estrategia mejor
"""

def cruce_medias(ticker, period='1min',long_window=10*3,short_window=5):
    """
    Estrategia utilizando el cruce de dos medias. Para poder calcular las medias,
    primero debo generar el OHLC en el periodo especificado. A su vez, se caluclan
    las dos medias moviles simples para la ventana long y short. En funcion del cruce
    se determina si comprar o vender
    """
    start = datetime.now()
    df = utils.DBtools.read_ticker(ticker)
    print('Just read the database takes:', (datetime.now()-start).total_seconds())
##        df.index = pd.to_datetime(df.index)
    df = df[df['Ticker']== ticker].drop_duplicates('LA_date')
    print('Read the info from database and make a DataFrame takes:,'(datetime.now()-start).total_seconds())
    FS = 1.0   #Factor de seguridad = 2%?
    start = datetime.now()
    
    ohlc = getOHLC(df,period=period)
    ohlc.fillna(method='ffill', inplace=True)
    ohlc = ohlc.between_time('10:00','17:00')
    print('get the OHLC takes:,'(datetime.now()-start).total_seconds())
    start = datetime.now()
    estrategia = ohlc.copy()
    estrategia['SMA_short'] = ohlc.close.rolling(window=short_window).mean()
    estrategia['SMA_long'] = ohlc.close.rolling(window=long_window).mean()
    
    estrategia.loc[estrategia['SMA_short'] > estrategia['SMA_long']*FS, 'estrategia'] = 1
    estrategia.loc[estrategia['SMA_short'] < estrategia['SMA_long']/FS, 'estrategia'] = -1
    estrategia.estrategia.fillna(method='ffill', inplace=True)
    
    estrategia = estrategia.loc[estrategia.index > datetime.strptime('19-02-2019','%d-%m-%Y')]
    position =  estrategia.estrategia.iloc[-1]
    print('Calculate the strategy takes:,'(datetime.now()-start).total_seconds())
    
    if position == 1:
        price = df['OF_price'].iloc[-1]
        side = 'BUY'
    elif position == -1:
        price = df['BI_price'].iloc[-1]
        side = 'SELL'
    else:
        price = 0
        side = ''
    return position, price, side, estrategia

def stochastic(ticker, period='2min'):
    """
    Compra y venta en funcion del valor del estocastico. Si cruza hacia abajo de 80, es venta, si cruza hacia arriba de 20, es compra.
    """
    df = utils.DBtools.read_ticker(ticker)
    df = df[df['Ticker']== ticker].drop_duplicates('LA_date')
    ohlc = getOHLC(df,'LA_price','LA_size',period)
    # ohlc.dropna(inplace=True)
    ohlc.drop(['open','high','low','volumen'], axis = 1,inplace = True)
    ohlc.fillna(method='ffill', inplace=True)
    
    stoch = indicadores.stochastic(ohlc, 'close')
    ohlc = pd.concat([ohlc, stoch], axis=1, join_axes=[ohlc.index])
    
    ohlc.loc[(stoch.d < 20) & (stoch.d.shift(-1) > 20), 'estrategia'] = 1
    ohlc.loc[(stoch.d > 80) & (stoch.d.shift(-1) < 80), 'estrategia'] = -1
    ohlc.fillna(method='ffill', inplace=True)
    
    ohlc.insert(len(ohlc.columns),'cum returns', ohlc.close.diff() * ohlc.estrategia)

    return ohlc

def mean_reversion(ticker,period='1min',window=5,start_date=''):
    """
    Mean reversion Strategy.
    Basically follows the moving average, and the standard deviatino of the sotck price, and look if is moment of buy or sell.
    More info in https://medium.com/auquan/mean-reversion-simple-trading-strategies-part-1-a18a87c1196a
    """
    df = utils.DBtools.read_ticker(ticker,start_date=start_date)
    df = df.drop_duplicates('LA_date')
    ohlc = getOHLC(df,'LA_price','LA_size',period)
    ohlc.dropna(inplace=True)

    ohlc['mean'] = ohlc.close.rolling(window = window).mean()
    ohlc['std'] = ohlc.close.rolling(window = window).std()
    ohlc['zscore']= (ohlc['close'] - ohlc['mean'])/ohlc['std']
    # Sell short if the z-score is > 1
    ohlc.loc[(ohlc.zscore > 1), 'estrategia'] = -1
    # Buy long if the z-score is < -1
    ohlc.loc[(ohlc.zscore < -1), 'estrategia'] = 1
    # Clear positions if the z-score between -.5 and .5
    ohlc.loc[(ohlc.zscore < 0.5) & (ohlc.zscore > -0.5), 'estrategia'] = 0
    ohlc['estrategia'].fillna(method='ffill', inplace=True)
    
    ohlc['returns'] = ohlc.close.diff() * ohlc.estrategia
    ohlc['Cumulative'] = ohlc.returns.cumsum()
        
    return ohlc
    
def plot_estrategia(df):
    """
    Graficador de la estrategia, para utilizar como backtest
    El DF que se debe ingresar, debe contener el precio de cierre (LA_price), y una columna estrategia (columna, con 1 0 -1 indicando compra neutro o venta)
    """
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    
    fig= plt.figure()

    ax = plt.subplot2grid((4,1), (0,0), rowspan=2, colspan=1)
    ax1 = plt.subplot2grid((4,1), (2,0), rowspan=1, colspan=1, sharex=ax)
    ax2 = plt.subplot2grid((4,1), (3,0), rowspan=1, colspan=1, sharex=ax)
    
    df.dropna(inplace=True)
    # print(df)
    df.close.plot(ax=ax)
    if 'tasa_implicita' in df.columns:
        ax3 = ax.twinx()
        df['tasa_implicita'].plot(ax=ax3,color='red')
    if 'SMA_long' in df.columns:
        df[['SMA_long','SMA_short']].plot(ax=ax)
    ax.fill_between(df.index,df['close'].min()*0.95, df['close'].max()*1.1, where=df['estrategia']==1, alpha =0.4, color='g')
    ax.fill_between(df.index,df['close'].min()*0.95, df['close'].max()*1.1, where=df['estrategia']==-1, alpha =0.4, color='r')
    
    df.estrategia.plot(ax=ax1)
    df.Cumulative.plot(ax=ax2)
        
    ax.xaxis.grid(True, which='major', alpha =0.6, linestyle='-')
    ax.xaxis.grid(True,which='minor',alpha=0.3, linestyle='--')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d - %H:%M'))
   
    
    plt.setp(ax.get_xticklabels(), visible=False)
    plt.setp(ax1.get_xticklabels(), visible=False)
    plt.setp(ax2.get_xticklabels(), visible=True)
    
    ax1.set_ylabel('Position')
    ax2.set_ylabel('Cum. Result')
    ax1.grid(True, alpha = 0.4)
    ax.xaxis.grid(True, alpha = 0.4)
    
    plt.show()

def arbitrador_tasa_backend(ticker_spot,ticker_futuro,ticker_tasa = '',tasa_referencia=0.4,fecha_vto="28-3-2019",fs = 0.02):
    """
    Arbitrador de tasa implicita, en función de una tasa de referencia.
    """
    
    df_spot = utils.DBtools.read_ticker(ticker_spot)
    df_futuro = utils.DBtools.read_ticker(ticker_futuro)
    tasa = indicadores.tasa_implicita(df_futuro=df_futuro,df_spot=df_spot,fecha_hoy=datetime.today(),fecha_vto=datetime.strptime(fecha_vto,'%d-%m-%Y'))
    estrategia = pd.DataFrame(tasa)

    estrategia.loc[estrategia.tasa_implicita > df_tasa_ref*(1+fs), 'estrategia'] = -1
    estrategia.loc[estrategia.tasa_implicita < df_tasa_ref*(1-fs), 'estrategia'] = 1
    #Si no cumple ninguna de las anteriores, relleno con HOLD
    estrategia.fillna(method='ffill', inplace=True)
    estrategia['Diff'] = (estrategia.close - estrategia.close.shift(1))*estrategia.estrategia
    estrategia['Cumulative'] = estrategia['Diff'].cumsum()
    # print(estrategia)
    position =  estrategia.estrategia.iloc[-1]
    returns = estrategia.Cumulative.iloc[-1]
    if position == 1:
        price = df_futuro['OF_price'].iloc[-1]
        side = 'BUY'
    elif position == -1:
        price = df_futuro['BI_price'].iloc[-1]
        side = 'SELL'
    else:
        price = 0
        side = 'HOLD'
    return position, price, side, returns ,estrategia

def arbitrador_tasa_online(ticker_spot,ticker_futuro,futuro_col_name= 'LA_price',tasa_referencia=0.4,fecha_vto="28-3-2019",fs = 0.02):
    """
    Arbitrador de tasa implicita, en función de una tasa de referencia. Donde solo me fijo en el ultimo valor de cotizacion y de tasa
    Inputs:
        ticker_spot: Is the name of the spot. For example, I.RFX20
        ticker_futuro: Is the name of the future to analize the implicit rate, For example: RFX20Mar19
        tasa_referencia: Is the reference rate. If the implicit rate is bigger than the reference rate, an short position will be send and viceversa.
        fecha_vto: Day for finishing the future, in string with format %d-%m-%Y: '28-3-2019'
        fs: Safety factor.
    Return:
        tasa: Actual implicit rate
        position: recomended position size
        side: side for the operatio, sell, buy or hold.
        price: recomended price for the operation.
        
        ISSUES: PRICE IS SETTED AS 5$ more (or less) than the BID side (offer side) for the buy (sell) position.
        5$ because pinto
    """
    try:
        df_spot = utils.DBtools.read_last_row(ticker_spot)
        df_futuro = utils.DBtools.read_last_row(ticker_futuro)
        tasa = indicadores.tasa_implicita_online(df_spot['LA_price'],df_futuro[futuro_col_name],fecha_hoy=datetime.today(),fecha_vto=datetime.strptime(fecha_vto,'%d-%m-%Y'))
        if tasa < tasa_referencia*(1+fs):
            position = 1
            side = 'BUY'
            try:
                price = min(df_futuro['BI_price']+5, df_futuro['OF_price']-5)
            except:
                price = df_futuro['BI_price']+5 if df_futuro['OF_price'] is None else df_futuro['OF_price']-5
        elif tasa > tasa_referencia*(1-fs):
            position = -1
            side = 'SELL'
            try:
                price = min(df_futuro['BI_price']+5, df_futuro['OF_price']-5)
            except:
                price = df_futuro['BI_price']+5 if df_futuro['OF_price'] is None else df_futuro['OF_price']-5
                #FIX ME!, falta restarle 5 pe!
        else:
            position = 0
            side = 'HOLD'
            price = price = df_futuro['LA_price']
        return tasa, position, side, price
    except:
        logging.exception("Exception performing the strategy")
        print('Ups!, error al implementar la estrategia. Para mas informacion revisar en el log')
        
def FollowTheVolume(ticker,start_date=datetime.today().strftime('%Y-%m-%d'), period='1min', no_operation_roc=0.005):
    start = datetime.now()
    df = utils.DBtools.read_ticker(ticker,start_date=start_date)
    df = df.drop_duplicates('LA_date')
    # print("get the df from the DB takes ", (datetime.now()-start).total_seconds(), "seconds")
    start = datetime.now()
    try:
        ohlc = getOHLC(df,'LA_price','LA_size',period)
        ohlc.dropna(inplace=True)
        # print("get the OHLC takes ", (datetime.now()-start).total_seconds(), "seconds")
        start = datetime.now()
        ohlc = indicadores.vwap(ohlc,column_price_name='close', column_volume_name='volume')
        # print("get the VWAP takes ", (datetime.now()-start).total_seconds(), "seconds")
        start = datetime.now()
        ohlc = indicadores.ROC(ohlc, column_price_name='VWAP')
        # print("get the ROC takes ", (datetime.now()-start).total_seconds(), "seconds")
        start = datetime.now()

        ohlc.loc[(ohlc.ROC < 0), 'estrategia'] = -1
        ohlc.loc[(ohlc.ROC > 0), 'estrategia'] = 1
        # ohlc.loc[(ohlc.ROC > -no_operation_roc) & (ohlc.ROC < no_operation_roc), 'estrategia'] = 0
        
        ohlc['estrategia'].fillna(method='ffill', inplace=True)
        # print("get the Strategy takes ", (datetime.now()-start).total_seconds(), "seconds")
        start = datetime.now()
        
        ohlc['returns'] = ohlc.close.diff() * ohlc.estrategia
        ohlc['Cumulative'] = ohlc.returns.cumsum()
    except:
        ohlc = pd.DataFrame()
    return ohlc

if __name__ == '__main__':
    # start = datetime.now()
    # estrategia, returns = cruce_medias('RFX20Mar19')
    # end = datetime.now()
    # print('Perform the cruce_medias strategy takes ', (end-start).total_seconds(), 'seconds')
    
    # start = datetime.now()
    # estrategia = mean_reversion('RFX20Jun19',period = '1min', window=50)
    # end = datetime.now()
    # print(estrategia.tail())
    # print('Perform the strategy takes ', (end-start).total_seconds(), 'seconds')
    # plot_estrategia(estrategia)
    
    
    start = datetime.now()
    estrategia = FollowTheVolume(ticker='RFX20Jun19',period = '1min',start_date=datetime.today())
    end = datetime.now()

    print('Perform the strategy takes ', (end-start).total_seconds(), 'seconds')
    # print(estrategia)
    plot_estrategia(estrategia)
    
    # for tasa_ref in range(10,70,5):
        # tasa_ref = tasa_ref/100
        # for fs in range (0,5,1):
            # fs = fs/100
            # start = datetime.now()
            # position, price, side, returns, estrategia = arbitrador_tasa_backend(ticker_spot='I.RFX20',ticker_futuro='RFX20Mar19',tasa_referencia=tasa_ref, fs = fs)
            # end = datetime.now()
            # print('Perform the arbitrador_tasa strategy takes ', (end-start).total_seconds(), 'seconds')
            # print('Tasa Ref:',tasa_ref,'FS',fs,'Returns:',returns)
    
    
    
    # for window in range(3,33,5):
        # for period in range(1,30,5):
            # period = str(period) + 'min'
            # estrategia = mean_reversion('RFX20Jun19',period = period, window=window)
            # print(period,window,estrategia.iloc[-1].Cumulative)
    
