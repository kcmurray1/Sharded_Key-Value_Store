from flask import Flask, request
import requests
import sys
import time
from bingus import create_app, views_route
from vectorclock import VectorClock
import threading

TIME_TO_BOOT = 2

def notify_replicas(view_points, socket_address):
    # view and vector clock at minimum should be
    # views = {a}
    # vc = {a: 0}
    # Add this replica to other replicas
    for view_address in view_points:
        while True:
            # Add Replicas that respond
            try:
                # Send PUT request
                response = requests.put(f'http://{view_address}/view', json={'view': socket_address, 'relay': True})
                metadata = response.json()
                views_route.views.add(view_address)

                # Check for replicated data in response
                if 'replica_data' in metadata:
                    # compare with padding
                    received_vc = metadata['replica_data']['vc']
                    # pad local clock {a: 0, b:0}
                    views_route.local_vc.update(received_vc)
                    # pad received clock {b: 0, a:0}
                    received_vc.update(views_route.local_vc)

                    # compare clocks(should equal length and share keys)
                    with views_route.update_views_lock:
                        if VectorClock(views_route.local_vc) <= VectorClock(received_vc):
                            views_route.local_vc = received_vc
                            views_route._store = metadata['replica_data']['store']
                break

            # Ignore unresponsive replicas
            except (requests.ConnectionError, requests.RequestException, requests.exceptions.HTTPError):
                break
    

    # Start Pulse Sender Thread
    h = threading.Timer(TIME_TO_BOOT * 2, pulse_starter)
    h.start()

# Build then run Replica
def startup():
    """Set initial replica values
    socket = addr
    views = {addr}
    local_clock = {addr: 0}
    """

    views_route.socket_address = sys.argv[1]
    views_route.views.add(views_route.socket_address)
    views_route.local_vc[views_route.socket_address] = 0
    
    starting_views = sys.argv[2].split(',')
    starting_views.remove(views_route.socket_address)

    # Notify other replicas about this new instance
    t = threading.Timer(TIME_TO_BOOT, notify_replicas, args=(starting_views, views_route.socket_address))
    t.start()

    my_app = create_app()
    # https://stackoverflow.com/questions/43644083/python-thread-running-twice-when-called-once-in-main
    my_app.run(host='0.0.0.0', port=8090, debug=False)

def pulse_starter():
    views_route.periodic_pulse_sender()

if __name__ == '__main__':
    startup()
