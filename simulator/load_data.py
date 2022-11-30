import pandas as pd
from typing import List, Optional
from .utils import AnonTrade, MdUpdate, OrderbookSnapshotUpdate


def load_md_interval(path: str,
                     min_ts: Optional[pd.Timestamp] = None,
                     max_ts: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    if min_ts is None and max_ts is None:
        df = pd.read_csv(path)
    else:
        chunksize = 100_000
        chunks = []
        for chunk in pd.read_csv(path, chunksize=chunksize):
            if min_ts is not None and chunk['receive_ts'].iloc[-1] < min_ts.value:
                pass
            if max_ts is not None and chunk['receive_ts'].iloc[0] > max_ts.value:
                break
            chunks.append(chunk)
        df = pd.concat(chunks)
        if min_ts is not None:
            mask = df['receive_ts'] >= min_ts.value
            df = df.loc[mask]
        if max_ts is not None:
            mask = df['receive_ts'] <= max_ts.value
            df = df.loc[mask]
    return df


def load_trades(path: str,
                min_ts: Optional[pd.Timestamp] = None,
                max_ts: Optional[pd.Timestamp] = None) -> List[AnonTrade]:
    trades = load_md_interval(path + 'trades.csv', min_ts, max_ts)
    # permute the columns to pass parameters to AnonTrade constructor
    trades = trades[
        ['exchange_ts', 'receive_ts', 'aggro_side', 'size', 'price']
    ].sort_values(['exchange_ts', 'receive_ts'])
    trades = [AnonTrade(*args) for args in trades.values]
    return trades


def load_books(path: str,
               min_ts: Optional[pd.Timestamp] = None,
               max_ts: Optional[pd.Timestamp] = None) -> List[OrderbookSnapshotUpdate]:
    lobs = load_md_interval(path + 'lobs.csv', min_ts, max_ts)

    # rename columns
    names = lobs.columns.values
    ln = len('btcusdt:Binance:LinearPerpetual_')
    renamer = {name: name[ln:] for name in names[2:]}
    renamer[' exchange_ts'] = 'exchange_ts'
    lobs.rename(renamer, axis=1, inplace=True)

    # timestamps
    receive_ts = lobs.receive_ts.values
    exchange_ts = lobs.exchange_ts.values
    # list of `ask_price`, `ask_vol` for different order book levels
    # shapes: len(asks) = 10, len(asks[0]) = len(lobs)
    asks = [list(zip(lobs[f"ask_price_{i}"], lobs[f"ask_vol_{i}"])) for i in range(10)]
    # transpose the list
    asks = [[asks[i][j] for i in range(len(asks))] for j in range(len(asks[0]))]
    # same for bids
    bids = [list(zip(lobs[f"bid_price_{i}"], lobs[f"bid_vol_{i}"])) for i in range(10)]
    bids = [[bids[i][j] for i in range(len(bids))] for j in range(len(bids[0]))]

    books = list(OrderbookSnapshotUpdate(*args) for args in zip(exchange_ts, receive_ts, asks, bids))
    return books


def merge_books_and_trades(books: List[OrderbookSnapshotUpdate],
                           trades: List[AnonTrade]) -> List[MdUpdate]:
    """ This function merges lists of orderbook snapshots and trades """
    trades_dict = {(trade.exchange_ts, trade.receive_ts): trade for trade in trades}
    books_dict = {(book.exchange_ts, book.receive_ts): book for book in books}

    ts = sorted(trades_dict.keys() | books_dict.keys())

    md = [MdUpdate(*key, books_dict.get(key, None), trades_dict.get(key, None)) for key in ts]
    return md


def load_md_from_file(path: str,
                      min_ts: Optional[pd.Timestamp] = None,
                      max_ts: Optional[pd.Timestamp] = None) -> List[MdUpdate]:
    """Load market data from specified time interval.

    Args:
        path:
            Path to the directory with market data.
        min_ts:
            If provided, market data with reception timestamps less
            than `min_ts` will not be included in resulting data frame.
        max_ts:
            If provided, market data with reception timestamps greater
            than `max_ts` will not be included in resulting data frame.

    Returns:
        A data frame with market data.
    """
    books = load_books(path, min_ts, max_ts)
    trades = load_trades(path, min_ts, max_ts)
    return merge_books_and_trades(books, trades)
