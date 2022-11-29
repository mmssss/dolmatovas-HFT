from typing import List, Union

import numpy as np
import pandas as pd

from .simulator import MdUpdate, OwnTrade, update_best_positions


def get_pnl(updates_list: List[Union[MdUpdate, OwnTrade]], cost=-0.00001) -> pd.DataFrame:
    """ This function calculates PnL from list of updates """
    base_pos, quote_pos = 0.0, 0.0  # current position in base and quote assets
    volume = 0.0  # current trading volume in quote asset
    N = len(updates_list)
    base_pos_arr = np.zeros((N,))
    quote_pos_arr = np.zeros((N,))
    mid_price_arr = np.zeros((N,))
    volume_arr = np.zeros((N,))
    # current best_bid and best_ask
    best_bid: float = -np.inf
    best_ask: float = np.inf

    for i, update in enumerate(updates_list):
        if isinstance(update, MdUpdate):
            best_bid, best_ask = update_best_positions(best_bid, best_ask, update)
        # mid price
        # i use it to calculate current portfolio value
        mid_price = 0.5 * (best_ask + best_bid)

        if isinstance(update, OwnTrade):
            trade = update
            volume += trade.size
            # update positions
            if trade.side == 'BID':
                base_pos += trade.size
                quote_pos -= trade.price * trade.size
            elif trade.side == 'ASK':
                base_pos -= trade.size
                quote_pos += trade.price * trade.size
            quote_pos -= cost * trade.price * trade.size

        base_pos_arr[i] = base_pos
        volume_arr[i] = volume
        quote_pos_arr[i] = quote_pos
        mid_price_arr[i] = mid_price

    worth_arr = base_pos_arr * mid_price_arr + quote_pos_arr
    receive_ts = [update.receive_ts for update in updates_list]
    exchange_ts = [update.exchange_ts for update in updates_list]

    df = pd.DataFrame({"exchange_ts": exchange_ts, "receive_ts": receive_ts,
                       "worth_quote": worth_arr, "volume": volume_arr,
                       "base_balance": base_pos_arr, "quote_balance": quote_pos_arr,
                       "mid_price": mid_price_arr})
    return df


def trade_to_dataframe(trades_list: List[OwnTrade]) -> pd.DataFrame:
    exchange_ts = [trade.exchange_ts for trade in trades_list]
    receive_ts = [trade.receive_ts for trade in trades_list]

    size = [trade.size for trade in trades_list]
    price = [trade.price for trade in trades_list]
    side = [trade.side for trade in trades_list]

    dct = {
        "exchange_ts": exchange_ts,
        "receive_ts": receive_ts,
        "size": size,
        "price": price,
        "side": side
    }

    # df = pd.DataFrame(dct).groupby('receive_ts').agg(lambda x: x.iloc[-1]).reset_index()
    df = pd.DataFrame(dct)
    return df


def md_to_dataframe(md_list: List[MdUpdate]) -> pd.DataFrame:
    best_bid = -np.inf
    best_ask = np.inf
    best_bids = []
    best_asks = []
    for md in md_list:
        best_bid, best_ask = update_best_positions(best_bid, best_ask, md)

        best_bids.append(best_bid)
        best_asks.append(best_ask)

    exchange_ts = [md.exchange_ts for md in md_list]
    receive_ts = [md.receive_ts for md in md_list]
    dct = {
        "exchange_ts": exchange_ts,
        "receive_ts": receive_ts,
        "bid_price": best_bids,
        "ask_price": best_asks
    }

    # df = pd.DataFrame(dct).groupby('receive_ts').agg(lambda x: x.iloc[-1]).reset_index()
    df = pd.DataFrame(dct)
    return df
