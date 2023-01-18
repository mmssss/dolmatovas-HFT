from collections import deque
from typing import List, Optional, Tuple, Union, Deque, Dict
from tqdm.auto import tqdm

import numpy as np

from .utils import Order, CancelOrder, OwnTrade, MdUpdate, \
    update_best_positions, PriorQueue


class Sim:
    def __init__(self, market_data: List[MdUpdate], execution_latency: float, md_latency: float) -> None:
        """Exchange simulator.
            Args:
                market_data(List[MdUpdate]): market data
                execution_latency(float): latency in nanoseconds
                md_latency(float): latency in nanoseconds
        """
        # TODO: add parameter to indicate whether our orders shold be
        #       last or first in queue on the level (whichever is less
        #       optimistic for specific strategy)
        # transform md to queue
        self.md_queue = deque(market_data)
        # action queue
        self.actions_queue: Deque[Union[Order, CancelOrder]] = deque()
        # SordetDict: receive_ts -> [updates]
        self.strategy_updates_queue = PriorQueue()
        # map : order_id -> Order
        self.ready_to_execute_orders: Dict[int, Order] = {}

        # current md
        self.md: Optional[MdUpdate] = None
        # current ids
        self.order_id = 0
        self.trade_id = 0
        # latency
        self.latency = execution_latency
        self.md_latency = md_latency
        # current bid and ask
        self.best_bid = -np.inf
        self.best_ask = np.inf
        # current trade
        self.trade_price = {'BID': -np.inf, 'ASK': np.inf}
        # last order
        self.last_order: Optional[Order] = None
        self.progress_bar = tqdm(total=len(self.md_queue))

    def get_md_queue_event_time(self) -> np.float:
        return np.inf if len(self.md_queue) == 0 else self.md_queue[0].exchange_ts

    def get_actions_queue_event_time(self) -> np.float:
        return np.inf if len(self.actions_queue) == 0 else self.actions_queue[0].exchange_ts

    def get_strategy_updates_queue_event_time(self) -> np.float:
        return self.strategy_updates_queue.min_key()

    def get_order_id(self) -> int:
        res = self.order_id
        self.order_id += 1
        return res

    def get_trade_id(self) -> int:
        res = self.trade_id
        self.trade_id += 1
        return res

    def update_last_trade(self) -> None:
        assert self.md is not None, "no current market data!"
        if self.md.trade is not None:
            self.trade_price[self.md.trade.side] = self.md.trade.price

    def delete_last_trade(self) -> None:
        self.trade_price['BID'] = -np.inf
        self.trade_price['ASK'] = np.inf

    def update_md(self, md: MdUpdate) -> None:
        # current orderbook
        self.md = md
        # update position
        self.best_bid, self.best_ask = update_best_positions(self.best_bid, self.best_ask, md)
        # update info about last trade
        self.update_last_trade()

        # add md to strategy_updates_queue
        self.strategy_updates_queue.push(md.receive_ts, md)

    def update_action(self, action: Union[Order, CancelOrder]) -> None:

        if isinstance(action, Order):
            # self.ready_to_execute_orders[action.order_id] = action
            # save last order to try to execute it aggressively
            self.last_order = action
        elif isinstance(action, CancelOrder):
            # cancel order
            if action.id_to_delete in self.ready_to_execute_orders:
                self.ready_to_execute_orders.pop(action.id_to_delete)
        else:
            assert False, "Wrong action type!"

    def tick(self) -> Tuple[float, List[Union[OwnTrade, MdUpdate]]]:
        """
            Simulation tick

            Returns:
                receive_ts(float): receive timestamp in nanoseconds
                res(List[Union[OwnTrade, MdUpdate]]): simulation result.
        """
        while True:
            # get event time for all the queues
            strategy_updates_queue_et = self.get_strategy_updates_queue_event_time()
            md_queue_et = self.get_md_queue_event_time()
            actions_queue_et = self.get_actions_queue_event_time()

            # if both queue are empty
            if md_queue_et == np.inf and actions_queue_et == np.inf:
                break

            # strategy queue has minimum event time
            if strategy_updates_queue_et < min(md_queue_et, actions_queue_et):
                break

            call_execute = md_queue_et <= actions_queue_et
            if md_queue_et <= actions_queue_et:
                self.update_md(self.md_queue.popleft())
                self.progress_bar.update(1)
            if actions_queue_et <= md_queue_et:
                self.update_action(self.actions_queue.popleft())
                # execute last order aggressively
                self.execute_last_order()

            # execute orders with current orderbook
            if call_execute:
                self.execute_orders()
            # delete last trade
            self.delete_last_trade()
        key, res = self.strategy_updates_queue.pop()
        return key, res

    def execute_last_order(self) -> None:
        """
            This function tries to execute self.last order aggressively.
        """
        # nothing to execute
        if self.last_order is None:
            return

        executed_price = None

        if self.last_order.side == 'BID' and self.last_order.price >= self.best_ask:
            executed_price = self.best_ask
        elif self.last_order.side == 'ASK' and self.last_order.price <= self.best_bid:
            executed_price = self.best_bid

        if executed_price is not None:
            if self.last_order.type != 'POST_ONLY':
                execute = 'BOOK'
                trade_type = 'TAKER'
                own_trade = OwnTrade(
                    self.last_order.place_ts,  # when we place the order
                    self.md.exchange_ts,
                    self.md.exchange_ts + self.md_latency,
                    self.get_trade_id(),
                    self.last_order.order_id,
                    self.last_order.side,
                    self.last_order.size,
                    executed_price,
                    trade_type,
                    execute)
                self.strategy_updates_queue.push(own_trade.receive_ts, own_trade)
        else:
            self.ready_to_execute_orders[self.last_order.order_id] = self.last_order

        # delete last order
        self.last_order = None

    def execute_orders(self) -> None:
        executed_orders_id = []
        for order_id, order in self.ready_to_execute_orders.items():
            executed_price, execute = None, None

            if order.side == 'BID' and order.price >= self.best_ask:
                executed_price = order.price
                execute = 'BOOK'
            elif order.side == 'ASK' and order.price <= self.best_bid:
                executed_price = order.price
                execute = 'BOOK'
            elif order.side == 'BID' and order.price >= self.trade_price['ASK']:
                executed_price = order.price
                execute = 'TRADE'
            elif order.side == 'ASK' and order.price <= self.trade_price['BID']:
                executed_price = order.price
                execute = 'TRADE'

            if executed_price is not None:
                trade_type = 'MAKER'
                executed_order = OwnTrade(
                    order.place_ts,  # when we place the order
                    self.md.exchange_ts,  # exchange ts
                    self.md.exchange_ts + self.md_latency,  # receive ts
                    self.get_trade_id(),  # trade id
                    order_id,
                    order.side,
                    order.size,
                    executed_price,
                    trade_type,
                    execute)
                executed_orders_id.append(order_id)
                # add order to strategy update queue
                self.strategy_updates_queue.push(executed_order.receive_ts, executed_order)

        # deleting executed orders
        for k in executed_orders_id:
            self.ready_to_execute_orders.pop(k)

    def place_order(self, ts: float, size: float, side: str,
                    price: float, order_type: str = 'LIMIT') -> Order:
        # добавляем заявку в список всех заявок
        order = Order(ts, ts + self.latency, self.get_order_id(), side, size,
                      price, order_type)
        self.actions_queue.append(order)
        return order

    def cancel_order(self, ts: float, id_to_delete: int) -> CancelOrder:
        # добавляем заявку на удаление
        ts += self.latency
        delete_order = CancelOrder(ts, id_to_delete)
        self.actions_queue.append(delete_order)
        return delete_order
