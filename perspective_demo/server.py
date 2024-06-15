#  ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
#  ┃ ██████ ██████ ██████       █      █      █      █      █ █▄  ▀███ █       ┃
#  ┃ ▄▄▄▄▄█ █▄▄▄▄▄ ▄▄▄▄▄█  ▀▀▀▀▀█▀▀▀▀▀ █ ▀▀▀▀▀█ ████████▌▐███ ███▄  ▀█ █ ▀▀▀▀▀ ┃
#  ┃ █▀▀▀▀▀ █▀▀▀▀▀ █▀██▀▀ ▄▄▄▄▄ █ ▄▄▄▄▄█ ▄▄▄▄▄█ ████████▌▐███ █████▄   █ ▄▄▄▄▄ ┃
#  ┃ █      ██████ █  ▀█▄       █ ██████      █      ███▌▐███ ███████▄ █       ┃
#  ┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
#  ┃ Copyright (c) 2017, the Perspective Authors.                              ┃
#  ┃ ╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌ ┃
#  ┃ This file is part of the Perspective library, distributed under the terms ┃
#  ┃ of the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). ┃
#  ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

import random
import logging
import threading
import tornado.websocket
import tornado.web
import tornado.ioloop
from datetime import date, datetime
from perspective import Table, PerspectiveManager, PerspectiveTornadoHandler
import threading
import pyarrow as pa
import pyarrow.parquet as pq
import concurrent.futures
import random


def data_source():
    table = pq.read_table('flights.parquet')

    next_table = pa.Table.from_arrays(arrays=pa.compute.add(table.column('DEP_DELAY'), random.randint(1, 100)), names=['DEP_DELAY'])

    count = count + 1
    stream = pa.BufferOutputStream()
    writer = pa.RecordBatchStreamWriter(stream, next_table.schema, use_legacy_format=False)

    writer.write_table(next_table)

    return stream.getvalue().to_pybytes()




IS_MULTI_THREADED = True
SECURITIES = [
    "AAPL.N",
    "AMZN.N",
    "QQQ.N",
    "NVDA.N",
    "TSLA.N",
    "FB.N",
    "MSFT.N",
    "TLT.N",
    "XIV.N",
    "YY.N",
    "CSCO.N",
    "GOOGL.N",
    "PCLN.N",
]

CLIENTS = ["Homer", "Marge", "Bart", "Lisa", "Maggie", "Moe", "Lenny", "Carl", "Krusty"]


def perspective_thread(manager):
    """Perspective application thread starts its own tornado IOLoop, and
    adds the table with the name "data_source_one", which will be used
    in the front-end."""
    table = Table(
        {
            "FL_DATE" : date,
            "DEP_DELAY" : int,
            "ARR_DELAY" : int,
            "AIR_TIME" : int,
            "DISTANCE" : int,
            "DEP_TIME" : float,
            "ARR_TIME" : float
        },
        limit=2500,
    )

    # Track the table with the name "data_source_one", which will be used in
    # the front-end to access the Table.
    manager.host_table("data_source_one", table)

    atable = pq.read_table('flights.parquet')

    stream = pa.BufferOutputStream()
    writer = pa.RecordBatchStreamWriter(stream, atable.schema, use_legacy_format=False)

    writer.write_table(atable)

    table.update(stream.getvalue().to_pybytes())

    # update with new data every 50ms
    def updater():
        table.update(data_source())

    callback = tornado.ioloop.PeriodicCallback(callback=updater, callback_time=5000)

    psp_loop = tornado.ioloop.IOLoop()
    if IS_MULTI_THREADED:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            manager.set_loop_callback(psp_loop.run_in_executor, executor)
            callback.start()
            psp_loop.start()
    else:
        manager.set_loop_callback(psp_loop.add_callback)
        callback.start()
        psp_loop.start()


def make_app():
    manager = PerspectiveManager()

    thread = threading.Thread(target=perspective_thread, args=(manager,))
    thread.daemon = True
    thread.start()

    return tornado.web.Application(
        [
            # create a websocket endpoint that the client Javascript can access
            (
                r"/websocket",
                PerspectiveTornadoHandler,
                {"manager": manager, "check_origin": True},
            ),
            (
                r"/node_modules/(.*)",
                tornado.web.StaticFileHandler,
                {"path": "../../node_modules/"},
            ),
            (
                r"/(.*)",
                tornado.web.StaticFileHandler,
                {"path": "./", "default_filename": "index.html"},
            ),
        ]
    )


if __name__ == "__main__":
    app = make_app()
    app.listen(8080)
    logging.critical("Listening on http://localhost:8080")
    loop = tornado.ioloop.IOLoop.current()
    loop.start()