#!/usr/bin/env python3
"""
Quick script to trigger manual state verification in bridge2
"""
import asyncio
import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

async def trigger_verification():
    """Connect to bridge2 and trigger manual verification"""
    try:
        # Import the bridge class
        from bridge2 import ZenMQTTBridge2
        
        # Create bridge instance
        bridge = ZenMQTTBridge2()
        
        # Load config
        await bridge.load_config()
        
        # Setup Zen connection
        await bridge.setup_zen()
        
        # Connect to Zen controllers
        await bridge.zen.connect()
        
        print("🔍 Triggering manual state verification...")
        
        # Trigger manual verification
        await bridge.verify_all_states()
        
        print("✅ Manual verification complete")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(trigger_verification())
