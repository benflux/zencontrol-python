#!/usr/bin/env python3
"""
Script to manually trigger state verification
"""
import asyncio
import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

async def main():
    """Manually trigger state verification"""
    try:
        # Import zencontrol
        import zencontrol
        
        # Create a ZenControl instance
        zen = zencontrol.ZenControl()
        
        # Add controllers from config
        zen.add_controller(
            id=0,
            name="zen1", 
            label="Zencontrol 1",
            host="192.168.1.150",
            port=5108,
            mac="7C:BA:CC:22:E3:4F"
        )
        zen.add_controller(
            id=1,
            name="zen2",
            label="Zencontrol 2", 
            host="192.168.1.193",
            port=5108,
            mac="7C:BA:CC:22:E3:7A"
        )
        zen.add_controller(
            id=2,
            name="zen3",
            label="Zencontrol 3",
            host="192.168.1.212", 
            port=5108,
            mac="7C:BA:CC:22:E3:8D"
        )
        
        # Wait for controllers to be ready
        for ctrl in zen.controllers:
            print(f"Waiting for controller {ctrl.name} to be ready...")
            while not await ctrl.is_controller_ready():
                await asyncio.sleep(1)
            await ctrl.interview()
            print(f"Controller {ctrl.name} ready")
        
        print("🔍 Getting all lights...")
        lights = await zen.get_lights()
        
        print(f"Found {len(lights)} lights")
        
        # Find Laundry devices (lights and groups)
        laundry_lights = [light for light in lights if "Laundry" in light.label]
        laundry_groups = []
        
        # Get groups too
        groups = await zen.get_groups()
        laundry_groups = [group for group in groups if "Laundry" in group.label]
        
        print(f"Found {len(laundry_lights)} Laundry lights and {len(laundry_groups)} Laundry groups:")
        
        for light in laundry_lights:
            print(f"  - Light: {light.label} (Address: {light.address.number})")
            print(f"    Current level: {light.level}")
            print(f"    Current colour: {light.colour}")
            print(f"    Current scene: {light.scene}")
            
            # Manually refresh state with verification
            print(f"    Refreshing state with verification...")
            await light.refresh_state_from_controller(verifying=True)
            
            print(f"    After refresh - Level: {light.level}, Colour: {light.colour}, Scene: {light.scene}")
            print()
        
        for group in laundry_groups:
            print(f"  - Group: {group.label} (Address: {group.address.number})")
            print(f"    Current level: {group.level}")
            print(f"    Current colour: {group.colour}")
            print(f"    Current scene: {group.scene}")
            
            # Manually refresh state with verification
            print(f"    Refreshing state with verification...")
            await group.refresh_state_from_controller(verifying=True)
            
            print(f"    After refresh - Level: {group.level}, Colour: {group.colour}, Scene: {group.scene}")
            print()
        
        print("✅ Manual verification complete")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
