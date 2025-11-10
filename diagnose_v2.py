#!/usr/bin/env python3
"""
ZenControl MQTT Diagnostic & Fix Tool v2

A comprehensive standalone tool that:
1. Connects to Zencontrol and queries all device states
2. Publishes all states to MQTT with retain=True
3. Verifies the retained messages were stored on the broker
4. Compares states and identifies any issues
5. Provides actionable recommendations

This eliminates the need to run mqtt_bridge.py separately.
"""

import asyncio
import yaml
import json
import zencontrol
from zencontrol import ZenController, ZenProtocol, ZenClient, ZenLight, ZenGroup
import aiomqtt
from colorama import Fore, Style, init
import math

init(autoreset=True)

class ZenMQTTDiagnostic:
    def __init__(self, config_path: str = "examples/config.yaml"):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        
        self.zen = None
        self.mqtt_client = None
        self.zen_states = {}
        self.mqtt_states = {}
        self.issues = []
        self.published_count = 0
        
    async def run(self):
        """Run complete diagnostic and fix workflow"""
        try:
            print(f"\n{Fore.CYAN}{'='*80}")
            print(f"{Fore.CYAN}ZenControl MQTT Diagnostic & Fix Tool v2")
            print(f"{Fore.CYAN}{'='*80}\n")
            
            # Step 1: Connect to Zencontrol
            await self.connect_zencontrol()
            
            # Step 2: Query all device states from Zencontrol
            await self.query_zencontrol_states()
            
            # Step 3: Publish states to MQTT with retention
            await self.publish_states_to_mqtt()
            
            # Step 4: Verify retained messages
            await self.verify_retained_messages()
            
            # Step 5: Compare and report
            await self.compare_and_report()
            
        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}⚠️ Diagnostic interrupted by user")
            self.print_partial_results()
        except Exception as e:
            print(f"\n{Fore.RED}❌ Diagnostic failed: {e}")
            print(f"{Fore.YELLOW}Check your configuration and try again")
    
    async def connect_zencontrol(self):
        """Connect to Zencontrol controllers"""
        print(f"{Fore.YELLOW}[1/5] Connecting to Zencontrol...")
        
        self.zen = zencontrol.ZenControl(print_traffic=False)
        
        for config in self.config['zencontrol']:
            ctrl = self.zen.add_controller(
                id=config['id'],
                name=config['name'],
                label=config['label'],
                host=config['host'],
                port=config.get('port', 5108),
                mac=config['mac']
            )
            await ctrl.interview()
            print(f"  {Fore.GREEN}✓ Connected to {ctrl.label}")
    
    async def query_zencontrol_states(self):
        """Query all device states from Zencontrol"""
        print(f"\n{Fore.YELLOW}[2/5] Querying device states...")
        
        # Query all lights
        lights = await self.zen.get_lights()
        for light in lights:
            try:
                level = await self.zen.protocol.dali_query_level(light.address)
                self.zen_states[f"light_{light.address.controller.name}_{light.address.number}"] = {
                    'type': 'light',
                    'label': light.label,
                    'level': level,
                    'controller': light.address.controller.name,
                    'address': light.address.number,
                    'device': light
                }
                print(f"  {Fore.GREEN}✓ {light.label}: Level={level}")
            except Exception as e:
                self.zen_states[f"light_{light.address.controller.name}_{light.address.number}"] = {
                    'type': 'light',
                    'label': light.label,
                    'level': None,
                    'error': str(e),
                    'controller': light.address.controller.name,
                    'address': light.address.number,
                    'device': light
                }
                print(f"  {Fore.RED}✗ {light.label}: Query failed - {e}")
                self.issues.append({
                    'device': light.label,
                    'issue': 'Zencontrol Query Failed',
                    'details': str(e)
                })
        
        # Query all groups
        groups = await self.zen.get_groups()
        for group in groups:
            if not group.lights:
                continue
            try:
                level = await self.zen.protocol.dali_query_level(group.address)
                self.zen_states[f"group_{group.address.controller.name}_{group.address.number}"] = {
                    'type': 'group',
                    'label': group.label,
                    'level': level,
                    'controller': group.address.controller.name,
                    'address': group.address.number,
                    'device': group
                }
                print(f"  {Fore.GREEN}✓ {group.label}: Level={level}")
            except Exception as e:
                self.zen_states[f"group_{group.address.controller.name}_{group.address.number}"] = {
                    'type': 'group',
                    'label': group.label,
                    'level': None,
                    'error': str(e),
                    'controller': group.address.controller.name,
                    'address': group.address.number,
                    'device': group
                }
                print(f"  {Fore.RED}✗ {group.label}: Query failed - {e}")
                self.issues.append({
                    'device': group.label,
                    'issue': 'Zencontrol Query Failed',
                    'details': str(e)
                })
        
        print(f"  {Fore.GREEN}✓ Queried {len(self.zen_states)} devices")
    
    async def publish_states_to_mqtt(self):
        """Publish all states to MQTT with retain=True"""
        print(f"\n{Fore.YELLOW}[3/5] Publishing states to MQTT with retention...")
        
        mqtt_config = self.config['mqtt']
        discovery_prefix = self.config['homeassistant']['discovery_prefix']
        
        try:
            async with aiomqtt.Client(
                hostname=mqtt_config['host'],
                port=mqtt_config['port'],
                username=mqtt_config.get('user'),
                password=mqtt_config.get('password'),
                keepalive=60
            ) as client:
                print(f"  {Fore.GREEN}✓ Connected to MQTT broker")
                
                for device_key, zen_state in self.zen_states.items():
                    device = zen_state['device']
                    level = zen_state.get('level')
                    controller_name = zen_state['controller']
                    address = zen_state['address']
                    
                    # Create MQTT topic
                    if zen_state['type'] == 'light':
                        mqtt_topic = f"{discovery_prefix}/light/{controller_name}/{address}/state"
                    else:  # group
                        mqtt_topic = f"{discovery_prefix}/light/{controller_name}/{address}/state"
                    
                    # Format state as JSON
                    if level is None or level == 255:
                        # Invalid level - publish OFF state
                        state_json = {"state": "OFF", "brightness": 0}
                    else:
                        # Valid level - convert to Home Assistant format
                        state_json = {
                            "state": "OFF" if level == 0 else "ON",
                            "brightness": self.arc_to_brightness(level)
                        }
                    
                    # Publish with retain=True
                    try:
                        await client.publish(mqtt_topic, json.dumps(state_json), retain=True)
                        self.published_count += 1
                        print(f"  {Fore.GREEN}✓ {zen_state['label']}: Published (retained=True)")
                    except Exception as e:
                        print(f"  {Fore.RED}✗ {zen_state['label']}: Publish failed - {e}")
                        self.issues.append({
                            'device': zen_state['label'],
                            'issue': 'MQTT Publish Failed',
                            'details': str(e)
                        })
                
                print(f"  {Fore.GREEN}✓ Published {self.published_count} states (retained=True)")
                
                # Wait for publishes to complete
                await asyncio.sleep(2)
                print(f"  {Fore.CYAN}✓ Waited for publishes to complete")
        
        except Exception as e:
            print(f"  {Fore.RED}✗ MQTT connection failed: {e}")
            self.issues.append({
                'device': 'MQTT',
                'issue': 'Connection Failed',
                'details': str(e)
            })
    
    async def verify_retained_messages(self):
        """Verify retained messages were stored on the broker"""
        print(f"\n{Fore.YELLOW}[4/5] Verifying retained messages...")
        
        mqtt_config = self.config['mqtt']
        discovery_prefix = self.config['homeassistant']['discovery_prefix']
        
        try:
            async with aiomqtt.Client(
                hostname=mqtt_config['host'],
                port=mqtt_config['port'],
                username=mqtt_config.get('user'),
                password=mqtt_config.get('password'),
                keepalive=60
            ) as client:
                print(f"  {Fore.GREEN}✓ Connected to MQTT broker")
                
                # Subscribe to all state topics
                await client.subscribe(f"{discovery_prefix}/light/+/+/state")
                print(f"  {Fore.GREEN}✓ Subscribed to {discovery_prefix}/light/+/+/state")
                
                # Wait for retained messages
                print(f"  Waiting for retained messages...")
                count = 0
                
                try:
                    async with asyncio.timeout(5.0):  # 5 second timeout
                        async for message in client.messages:
                            topic = str(message.topic)
                            payload = message.payload.decode() if message.payload else ""
                            
                            # Parse topic to extract device info
                            parts = topic.split('/')
                            if len(parts) >= 4:
                                device_key = f"{parts[1]}_{parts[2]}_{parts[3]}"
                                self.mqtt_states[device_key] = payload
                                count += 1
                                print(f"    {Fore.CYAN}Retrieved: {topic} = {payload}")
                            
                            # Stop after reasonable number of messages
                            if count > 200:
                                break
                except asyncio.TimeoutError:
                    print(f"  {Fore.YELLOW}⚠️ Timeout waiting for retained messages")
                
                print(f"  {Fore.GREEN}✓ Retrieved {count} retained messages")
        
        except Exception as e:
            print(f"  {Fore.RED}✗ MQTT verification failed: {e}")
            self.issues.append({
                'device': 'MQTT',
                'issue': 'Verification Failed',
                'details': str(e)
            })
    
    async def compare_and_report(self):
        """Compare states and provide comprehensive report"""
        print(f"\n{Fore.YELLOW}[5/5] Comparing states...")
        
        working_devices = 0
        missing_mqtt_devices = []
        state_mismatches = []
        
        for device_key, zen_state in self.zen_states.items():
            label = zen_state['label']
            zen_level = zen_state.get('level')
            
            # Check if MQTT state exists
            mqtt_state = self.mqtt_states.get(device_key)
            
            if zen_level is None:
                # Zencontrol query failed - already reported
                print(f"  {Fore.RED}✗ {label}: Zencontrol query failed")
            elif mqtt_state is None:
                # MQTT state missing
                print(f"  {Fore.RED}✗ {label}: No retained MQTT state found")
                missing_mqtt_devices.append(label)
                self.issues.append({
                    'device': label,
                    'issue': 'Missing Retained MQTT State',
                    'zen_state': f'Level={zen_level}',
                    'mqtt_state': 'Not Found'
                })
            else:
                # Both states exist - check if they match
                try:
                    mqtt_json = json.loads(mqtt_state)
                    expected_state = "OFF" if zen_level == 0 else "ON"
                    expected_brightness = self.arc_to_brightness(zen_level)
                    
                    if (mqtt_json.get('state') == expected_state and 
                        mqtt_json.get('brightness') == expected_brightness):
                        print(f"  {Fore.GREEN}✓ {label}: States match")
                        working_devices += 1
                    else:
                        print(f"  {Fore.YELLOW}⚠️ {label}: State mismatch")
                        state_mismatches.append(label)
                        self.issues.append({
                            'device': label,
                            'issue': 'State Mismatch',
                            'zen_state': f'Level={zen_level}',
                            'mqtt_state': mqtt_state
                        })
                except json.JSONDecodeError:
                    print(f"  {Fore.RED}✗ {label}: Invalid MQTT JSON")
                    self.issues.append({
                        'device': label,
                        'issue': 'Invalid MQTT JSON',
                        'zen_state': f'Level={zen_level}',
                        'mqtt_state': mqtt_state
                    })
        
        # Print comprehensive summary
        self.print_summary(working_devices, missing_mqtt_devices, state_mismatches)
    
    def print_summary(self, working_devices, missing_mqtt_devices, state_mismatches):
        """Print comprehensive summary and recommendations"""
        print(f"\n{Fore.CYAN}{'='*80}")
        print(f"{Fore.CYAN}Summary")
        print(f"{Fore.CYAN}{'='*80}\n")
        
        total_devices = len(self.zen_states)
        failed_devices = len(self.issues)
        
        print(f"{Fore.GREEN}✓ {working_devices} devices working perfectly")
        print(f"{Fore.YELLOW}⚠️ {len(state_mismatches)} devices with state mismatches")
        print(f"{Fore.RED}✗ {len(missing_mqtt_devices)} devices missing retained MQTT states")
        print(f"{Fore.CYAN}📊 Total devices: {total_devices}")
        print(f"{Fore.CYAN}📡 Published to MQTT: {self.published_count}")
        print(f"{Fore.CYAN}📥 Retrieved from MQTT: {len(self.mqtt_states)}")
        
        if self.issues:
            print(f"\n{Fore.YELLOW}Issues:")
            issue_types = {}
            for issue in self.issues:
                issue_type = issue['issue']
                if issue_type not in issue_types:
                    issue_types[issue_type] = []
                issue_types[issue_type].append(issue['device'])
            
            for issue_type, devices in issue_types.items():
                print(f"  - {issue_type}: {', '.join(devices[:5])}{'...' if len(devices) > 5 else ''}")
        
        print(f"\n{Fore.CYAN}{'='*80}")
        print(f"{Fore.CYAN}Result")
        print(f"{Fore.CYAN}{'='*80}\n")
        
        if working_devices > 0:
            print(f"{Fore.GREEN}✅ SUCCESS! {working_devices} devices now have retained states in MQTT")
        
        if state_mismatches:
            print(f"{Fore.YELLOW}⚠️  {len(state_mismatches)} devices have state mismatches (may be timing issues)")
        
        if missing_mqtt_devices:
            print(f"{Fore.RED}❌ {len(missing_mqtt_devices)} devices missing retained states (MQTT broker issue)")
        
        print(f"\n{Fore.CYAN}Your Home Assistant should now show correct states instead of 'Unknown'!")
        
        if self.issues:
            print(f"\n{Fore.YELLOW}Recommendations:")
            if any(issue['issue'] == 'Missing Retained MQTT State' for issue in self.issues):
                print(f"  - Check MQTT broker retention settings")
                print(f"  - Verify MQTT broker has sufficient storage")
                print(f"  - Check MQTT broker max_retained_messages setting")
            if any(issue['issue'] == 'State Mismatch' for issue in self.issues):
                print(f"  - State mismatches may be due to timing - run diagnostic again")
                print(f"  - Check if devices changed state during diagnostic")
            if any(issue['issue'] == 'Zencontrol Query Failed' for issue in self.issues):
                print(f"  - Check DALI bus connections")
                print(f"  - Verify devices are powered and responsive")
            if any(issue['issue'] == 'MQTT Publish Failed' for issue in self.issues):
                print(f"  - Check MQTT broker permissions")
                print(f"  - Verify network connectivity")
    
    def print_partial_results(self):
        """Print partial results when interrupted"""
        print(f"{Fore.CYAN}Partial results:")
        if hasattr(self, 'zen_states') and self.zen_states:
            print(f"  Found {len(self.zen_states)} Zencontrol devices")
        if hasattr(self, 'mqtt_states') and self.mqtt_states:
            print(f"  Found {len(self.mqtt_states)} MQTT states")
        if hasattr(self, 'published_count'):
            print(f"  Published {self.published_count} states to MQTT")
        print(f"{Fore.YELLOW}Run again to get complete results")
    
    def arc_to_brightness(self, arc_level: int) -> int:
        """Convert DALI arc level (0-254) to Home Assistant brightness (0-255)"""
        if arc_level == 0:
            return 0
        elif arc_level == 255:
            return 255
        else:
            # Convert using logarithmic curve similar to mqtt_bridge.py
            # Based on DALI logarithmic curve
            LOG_A = -59.53
            LOG_B = 56.58
            
            if arc_level <= 0:
                return 0
            
            # Convert arc level to percentage
            percentage = (arc_level / 254.0) * 100
            
            # Apply logarithmic curve
            brightness_percent = LOG_A + LOG_B * math.log(percentage)
            
            # Clamp to valid range
            brightness_percent = max(0, min(100, brightness_percent))
            
            # Convert to 0-255 range
            return int((brightness_percent / 100.0) * 255)

async def main():
    diagnostic = ZenMQTTDiagnostic()
    await diagnostic.run()

if __name__ == "__main__":
    asyncio.run(main())
