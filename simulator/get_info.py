from typing import List, Union

import numpy as np
import pandas as pd

from .simulator import MdUpdate, OwnTrade, update_best_positions


def get_metrics(updates_list: List[Union[MdUpdate, OwnTrade]], fee) -> pd.DataFrame:
    """Calculate PnL and other metrics from list of updates returned by the strategy.
    Args:
        updates_list: list of updates as returned by the strategy.
        fee: Maker/taker fee (a fraction, not percent)
    Returns:
        Data frame with PnL and other statistics.
    """
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
        # mid-price is used to calculate current portfolio value
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
            # on Binance futures, fees are deducted like this
            quote_pos -= fee * trade.price * trade.size

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
    df['exchange_ts'] = pd.to_datetime(df['exchange_ts'])
    df['receive_ts'] = pd.to_datetime(df['receive_ts'])
    df = df.reset_index().drop(columns=['index'])
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
    df.receive_ts = pd.to_datetime(df.receive_ts)
    df.exchange_ts = pd.to_datetime(df.exchange_ts)
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
    df['mid_price'] = (df['bid_price'] + df['ask_price']) / 2
    df['exchange_ts'] = pd.to_datetime(df['exchange_ts'])
    df['receive_ts'] = pd.to_datetime(df['receive_ts'])
    return df
