#!/usr/bin/env python3
"""
Script to manually publish correct Laundry state to MQTT
"""
import asyncio
import json
import aiomqtt

async def publish_correct_laundry_state():
    """Manually publish correct Laundry state to MQTT"""
    try:
        # Connect to MQTT broker
        async with aiomqtt.Client(
            hostname="192.168.1.224",
            port=1883,
            username="zencontrol",
            password="controlzen"
        ) as client:
            
            # Publish Laundry group state (Level=220 = about 86% brightness)
            laundry_topic = "homeassistant/light/zen1/group6/state"
            laundry_state = {
                "state": "ON",
                "brightness": 220  # Level=220 from Zencontrol
            }
            
            print(f"📡 Publishing correct Laundry state to {laundry_topic}: {laundry_state}")
            await client.publish(laundry_topic, json.dumps(laundry_state), retain=True)
            
            # Also publish individual Laundry lights
            laundry_lights = [
                ("homeassistant/light/zen1/ecg20/state", 220),  # Laundry Light 20
                ("homeassistant/light/zen1/ecg21/state", 220),  # Laundry Light 21
                ("homeassistant/light/zen1/ecg22/state", 220),  # Laundry Light 22
            ]
            
            for topic, brightness in laundry_lights:
                state = {
                    "state": "ON",
                    "brightness": brightness
                }
                print(f"📡 Publishing Laundry light state to {topic}: {state}")
                await client.publish(topic, json.dumps(state), retain=True)
            
            print("✅ Correct Laundry state published successfully!")
            
    except Exception as e:
        print(f"❌ Error publishing Laundry state: {e}")

if __name__ == "__main__":
    asyncio.run(publish_correct_laundry_state())

