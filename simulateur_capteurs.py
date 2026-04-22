import time, json, random
import paho.mqtt.client as mqtt

client = mqtt.Client()
client.connect("localhost", 1883, 60)

while True:
    now = time.strftime('%Y-%m-%dT%H:%M:%SZ')
    temp = round(random.uniform(-50, 200), 2)
    vib = round(random.uniform(-1, 6), 2)

    msg_temp = json.dumps({
        "machine_id": "pipe-101",
        "valeur": temp,
        "timestamp": now,
        "type_capteur": "temperature"
    })

    msg_vib = json.dumps({
        "machine_id": "pump-303",
        "valeur": vib,
        "timestamp": now,
        "type_capteur": "vibration"
    })

    client.publish("raffinerie/temp", msg_temp)
    client.publish("raffinerie/vib", msg_vib)

    print(f"Publiés → Temp: {temp}°C | Vib: {vib} mm/s")
    time.sleep(2)
