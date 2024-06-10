import yfinance as yf
import pandas as pd
import pickle
import datetime

class DividendInfo:
    def __init__(self, symbol, exchange=None, dividend_history=[], price_history=[], info={}):
        self._exchange = exchange
        self._symbol = symbol
        if exchange == 'TSE':
            self._symbol = f"{symbol}.TO"
        self._dividend_history = dividend_history
        self._price_history = price_history
        self._info = info

    def __str__(self):
        return f"Symbol: {self._symbol}"
    
    def retrieve_data(self):
        etf = yf.Ticker(self._symbol)
        dh = etf.dividends
        df = dh.reset_index()
        # check if the exchange is TSX
        if self._exchange == 'TSE':
            pass
            #df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
        #else:
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
        self._dividend_history = df
        self._price_history = etf.history("6mo")
        self._info = etf.info
    
    def __repr__(self):
        return f'DividendInfo(_symbol={self._symbol},_exchange={self._exchange},\
            _devidend_history={self._dividend_history},\
            _price_history={self._price_history}, _info={self._info})'

# build information
def parse_etf_list( file_path ):
    with open(file_path, 'r') as file:
        lines = file.read().splitlines()

    etf_list = []
    for i in range(len(lines)):
        uk, etf,stk,exchg, _ = lines[i].split(',')[:5]
        etf_list.append((etf, exchg.split('/')[1]))
    return etf_list




def build_info_list(DividendInfo, etf_list):
    etf_info_list = []
    etf_info_dict = {}
    for i , info in enumerate(etf_list):
        di = DividendInfo(info[0], info[1])
        di.retrieve_data()
        etf_info_list.append(di)
        k = di._symbol
        etf_info_dict[k] = di
        print(f"{i+1}/{len(etf_list)}: {k} processed.")
    return etf_info_list, etf_info_dict

def process_file(DividendInfo, parse_etf_list, build_info_list, infile, outfile):
    etf_list = parse_etf_list(infile)

    etf_info_list, ei_dict = build_info_list(DividendInfo, etf_list)
    # Save the list of objects to a file
    with open(outfile, 'wb') as fp:
        #pickle.dump(etf_info_list, fp)
        pickle.dump(ei_dict, fp)

if __name__ == '__main__':
    import os
    now = datetime.datetime.now()
    now_str = now.strftime("%Y-%m-%d")
    # walk through the data folder and process each file
    folder_in = "data_in"
    folder_out = "data_out"
    for file in os.listdir(folder_in):
        if file.endswith(".csv"):
            file_noext = file.replace(".csv", "")
            out_file = f"{folder_out}/{file_noext}_{now_str}.pkl"
            process_file(DividendInfo, parse_etf_list, build_info_list, f"{folder_in}/{file}", out_file)

    #file = "data/export_HiY2.csv"
    #process_file(DividendInfo, parse_etf_list, build_info_list, file, now_str=now_str)