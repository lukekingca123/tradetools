"""
事件引擎模块
"""
from collections import defaultdict
from queue import Empty, Queue
from threading import Thread
from time import sleep
from typing import Any, Callable, List

class Event:
    """事件对象"""

    def __init__(self, type: str, data: Any = None) -> None:
        """构造函数"""
        self.type: str = type    # 事件类型
        self.data: Any = data    # 事件数据

class EventEngine:
    """事件引擎"""

    def __init__(self) -> None:
        """构造函数"""
        self._queue: Queue = Queue()  # 事件队列
        self._active: bool = False    # 事件引擎开关
        self._thread: Thread = Thread(target=self._run)  # 事件处理线程
        self._handlers: defaultdict = defaultdict(list)  # 事件处理函数字典

    def _run(self) -> None:
        """引擎运行"""
        while self._active:
            try:
                event = self._queue.get(block=True, timeout=1)
                self._process(event)
            except Empty:
                pass

    def _process(self, event: Event) -> None:
        """处理事件"""
        if event.type in self._handlers:
            [handler(event) for handler in self._handlers[event.type]]

    def start(self) -> None:
        """启动引擎"""
        self._active = True
        self._thread.start()

    def stop(self) -> None:
        """停止引擎"""
        self._active = False
        self._thread.join()

    def register(self, type: str, handler: Callable[[Event], None]) -> None:
        """注册事件处理函数"""
        handler_list = self._handlers[type]
        if handler not in handler_list:
            handler_list.append(handler)

    def unregister(self, type: str, handler: Callable[[Event], None]) -> None:
        """注销事件处理函数"""
        handler_list = self._handlers[type]
        if handler in handler_list:
            handler_list.remove(handler)

        if not handler_list:
            self._handlers.pop(type)

    def put(self, event: Event) -> None:
        """推送事件"""
        self._queue.put(event)
