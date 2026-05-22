import os
import sys
import requests
import json

TOXIPROXY_API = os.getenv("TOXIPROXY_URL", "http://localhost:8474")
PROXY_NAME = "mqtt_proxy"
LISTEN_ADDR = "0.0.0.0:1884"
UPSTREAM_ADDR = "localhost:1883"

def setup_proxy():
    # Create proxy if it doesn't exist
    resp = requests.post(f"{TOXIPROXY_API}/proxies", json={
        "name": PROXY_NAME,
        "listen": LISTEN_ADDR,
        "upstream": UPSTREAM_ADDR
    })
    if resp.status_code == 201:
        print(f"Proxy {PROXY_NAME} created.")
    elif resp.status_code == 409:
        print(f"Proxy {PROXY_NAME} already exists.")
    else:
        print(f"Failed to create proxy: {resp.text}")
        sys.exit(1)

def disconnect():
    # Disabling the proxy itself kills all active connections immediately
    # and stops the proxy from accepting new ones.
    resp = requests.post(f"{TOXIPROXY_API}/proxies/{PROXY_NAME}", json={"enabled": False})
    if resp.status_code == 200:
        print("Proxy DISABLED - Network is DOWN.")
    else:
        print(f"Failed to disconnect: {resp.text}")

def reconnect():
    # Re-enabling the proxy restores the listener
    resp = requests.post(f"{TOXIPROXY_API}/proxies/{PROXY_NAME}", json={"enabled": True})
    if resp.status_code == 200:
        print("Proxy ENABLED - Network is UP.")
    else:
        print(f"Failed to reconnect: {resp.text}")

def slow():
    # Add 2 seconds of latency and limit bandwidth to 1KB/s
    # This simulates a very poor/congested connection
    requests.post(f"{TOXIPROXY_API}/proxies/{PROXY_NAME}/toxics", json={
        "name": "latency_toxic",
        "type": "latency",
        "stream": "downstream",
        "attributes": {"latency": 2000, "jitter": 500}
    })
    requests.post(f"{TOXIPROXY_API}/proxies/{PROXY_NAME}/toxics", json={
        "name": "bandwidth_toxic",
        "type": "bandwidth",
        "stream": "downstream",
        "attributes": {"rate": 1} # 1 KB/s
    })
    print("Proxy SLOWED - 2s latency, 1KB/s limit.")

def reset():
    # Remove all toxics and ensure enabled
    requests.post(f"{TOXIPROXY_API}/proxies/{PROXY_NAME}", json={"enabled": True})
    resp = requests.get(f"{TOXIPROXY_API}/proxies/{PROXY_NAME}/toxics")
    if resp.status_code == 200:
        for toxic in resp.json():
            requests.delete(f"{TOXIPROXY_API}/proxies/{PROXY_NAME}/toxics/{toxic['name']}")
    print("Proxy RESET - Clean state.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: manage_proxy.py [setup|disconnect|reconnect|slow|reset]")
        sys.exit(1)

    command = sys.argv[1]
    if command == "setup":
        setup_proxy()
    elif command == "disconnect":
        disconnect()
    elif command == "reconnect":
        reconnect()
    elif command == "slow":
        slow()
    elif command == "reset":
        reset()
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
