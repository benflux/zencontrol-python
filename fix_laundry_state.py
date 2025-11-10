#!/usr/bin/env python3
"""
Script to manually publish Laundry state to MQTT
"""
import asyncio
import json
import aiomqtt

async def publish_laundry_state():
    """Manually publish Laundry state to MQTT"""
    try:
        # Connect to MQTT broker
        async with aiomqtt.Client(
            hostname="192.168.1.224",
            port=1883,
            username="zencontrol",
            password="controlzen"
        ) as client:
            
            # Publish Laundry group state (100% brightness)
            laundry_topic = "homeassistant/light/zen1/group6/state"
            laundry_state = {
                "state": "ON",
                "brightness": 255  # 100% brightness
            }
            
            print(f"📡 Publishing Laundry state to {laundry_topic}: {laundry_state}")
            await client.publish(laundry_topic, json.dumps(laundry_state), retain=True)
            
            # Also publish individual Laundry lights
            for light_num in [20, 21, 22]:
                light_topic = f"homeassistant/light/zen1/ecg{light_num}/state"
                light_state = {
                    "state": "ON", 
                    "brightness": 255
                }
                
                print(f"📡 Publishing Laundry Light {light_num} state to {light_topic}: {light_state}")
                await client.publish(light_topic, json.dumps(light_state), retain=True)
            
            print("✅ Laundry state published successfully!")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(publish_laundry_state())

