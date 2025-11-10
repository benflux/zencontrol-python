#!/usr/bin/env python3
"""
Diagnostic script to identify devices with unknown states in Home Assistant
"""

import asyncio
import yaml
import zencontrol
from zencontrol import ZenController, ZenProtocol, ZenClient, ZenLight, ZenGroup
import aiomqtt
from colorama import Fore, Style, init

init(autoreset=True)

class StatesDiagnostic:
    def __init__(self, config_path: str = "examples/config.yaml"):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        
        self.zen = None
        self.mqtt_states = {}
        self.issues = []
    
    async def run(self):
        """Run full diagnostic"""
        try:
            print(f"\n{Fore.CYAN}{'='*80}")
            print(f"{Fore.CYAN}ZenControl State Diagnostic Tool")
            print(f"{Fore.CYAN}{'='*80}\n")
            
            # Step 1: Connect to Zencontrol
            await self.connect_zencontrol()
            
            # Step 2: Query all device states from Zencontrol
            await self.query_zencontrol_states()
            
            # Step 3: Query MQTT retained states
            await self.query_mqtt_states()
            
            # Step 4: Compare and identify issues
            await self.compare_states()
            
            # Step 5: Print report
            self.print_report()
            
            # Step 6: Provide recommendations
            self.print_recommendations()
            
        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}⚠️ Diagnostic interrupted by user")
            print(f"{Fore.CYAN}Partial results:")
            if hasattr(self, 'zen_states') and self.zen_states:
                print(f"  Found {len(self.zen_states)} Zencontrol devices")
            if hasattr(self, 'mqtt_states') and self.mqtt_states:
                print(f"  Found {len(self.mqtt_states)} MQTT states")
            print(f"{Fore.YELLOW}Run again to get complete results")
        except Exception as e:
            print(f"\n{Fore.RED}❌ Diagnostic failed: {e}")
            print(f"{Fore.YELLOW}Check your configuration and try again")
    
    async def connect_zencontrol(self):
        """Connect to Zencontrol controllers"""
        print(f"{Fore.YELLOW}[1/6] Connecting to Zencontrol...")
        
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
        """Query actual states from Zencontrol"""
        print(f"\n{Fore.YELLOW}[2/6] Querying device states from Zencontrol...")
        
        self.zen_states = {}
        
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
                }
                print(f"  {Fore.RED}✗ {light.label}: Query failed - {e}")
                self.issues.append({
                    'device': light.label,
                    'issue': 'Query Failed',
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
                }
                print(f"  {Fore.RED}✗ {group.label}: Query failed - {e}")
                self.issues.append({
                    'device': group.label,
                    'issue': 'Query Failed',
                    'details': str(e)
                })
    
    async def query_mqtt_states(self):
        """Query retained MQTT states"""
        print(f"\n{Fore.YELLOW}[3/6] Querying MQTT retained states...")
        
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
                
                # Use asyncio.wait_for with timeout
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
                                print(f"    {Fore.CYAN}Received: {topic} = {payload}")
                            
                            # Stop after reasonable number of messages
                            if count > 200:
                                break
                except asyncio.TimeoutError:
                    print(f"  {Fore.YELLOW}⚠️ Timeout waiting for retained messages")
                except KeyboardInterrupt:
                    print(f"  {Fore.YELLOW}⚠️ Interrupted by user")
                    raise  # Re-raise to exit gracefully
                
                print(f"  {Fore.GREEN}✓ Retrieved {count} MQTT states")
        
        except KeyboardInterrupt:
            print(f"  {Fore.YELLOW}⚠️ MQTT query interrupted by user")
            raise  # Re-raise to exit gracefully
        except Exception as e:
            print(f"  {Fore.RED}✗ MQTT query failed: {e}")
            print(f"  {Fore.YELLOW}Debug info:")
            print(f"    Host: {mqtt_config['host']}")
            print(f"    Port: {mqtt_config['port']}")
            print(f"    User: {mqtt_config.get('user', 'None')}")
            print(f"    Discovery prefix: {discovery_prefix}")
            self.issues.append({
                'device': 'MQTT',
                'issue': 'Connection Failed',
                'details': str(e)
            })
    
    async def compare_states(self):
        """Compare Zencontrol states with MQTT states"""
        print(f"\n{Fore.YELLOW}[4/6] Comparing states...")
        
        for device_key, zen_state in self.zen_states.items():
            label = zen_state['label']
            zen_level = zen_state.get('level')
            
            # Try to find corresponding MQTT state with better matching
            mqtt_state = None
            mqtt_topic = None
            
            # Try multiple matching strategies
            for mqtt_key, mqtt_value in self.mqtt_states.items():
                # Strategy 1: Direct label match
                if label.lower().replace(' ', '_') in mqtt_key.lower():
                    mqtt_state = mqtt_value
                    mqtt_topic = mqtt_key
                    break
                # Strategy 2: Partial match for complex names
                elif any(word.lower() in mqtt_key.lower() for word in label.split() if len(word) > 3):
                    mqtt_state = mqtt_value
                    mqtt_topic = mqtt_key
                    break
            
            # Check for mismatches
            if zen_level is None:
                print(f"  {Fore.RED}✗ {label}: Zencontrol query failed")
                self.issues.append({
                    'device': label,
                    'issue': 'Zencontrol Query Failed',
                    'zen_state': 'Unknown',
                    'mqtt_state': mqtt_state or 'Unknown'
                })
            elif mqtt_state is None:
                print(f"  {Fore.RED}✗ {label}: No MQTT state found")
                self.issues.append({
                    'device': label,
                    'issue': 'Missing MQTT State',
                    'zen_state': f'Level={zen_level}',
                    'mqtt_state': 'Not Published'
                })
            elif 'unknown' in mqtt_state.lower() or 'null' in mqtt_state.lower():
                print(f"  {Fore.RED}✗ {label}: MQTT shows Unknown")
                self.issues.append({
                    'device': label,
                    'issue': 'Unknown in Home Assistant',
                    'zen_state': f'Level={zen_level}',
                    'mqtt_state': mqtt_state
                })
            else:
                print(f"  {Fore.GREEN}✓ {label}: States match (MQTT: {mqtt_state})")
    
    def print_report(self):
        """Print diagnostic report"""
        print(f"\n{Fore.CYAN}{'='*80}")
        print(f"{Fore.CYAN}Diagnostic Report")
        print(f"{Fore.CYAN}{'='*80}\n")
        
        if not self.issues:
            print(f"{Fore.GREEN}✓ All devices are functioning correctly!")
            return
        
        print(f"{Fore.RED}Found {len(self.issues)} issue(s):\n")
        
        for i, issue in enumerate(self.issues, 1):
            print(f"{Fore.YELLOW}Issue #{i}: {issue['device']}")
            print(f"  Problem: {issue['issue']}")
            if 'zen_state' in issue:
                print(f"  Zencontrol State: {issue['zen_state']}")
            if 'mqtt_state' in issue:
                print(f"  MQTT/HA State: {issue['mqtt_state']}")
            if 'details' in issue:
                print(f"  Details: {issue['details']}")
            print()
    
    def print_recommendations(self):
        """Print recommendations"""
        print(f"\n{Fore.CYAN}{'='*80}")
        print(f"{Fore.CYAN}Recommendations")
        print(f"{Fore.CYAN}{'='*80}\n")
        
        if not self.issues:
            return
        
        # Group issues by type
        issue_types = {}
        for issue in self.issues:
            issue_type = issue['issue']
            if issue_type not in issue_types:
                issue_types[issue_type] = []
            issue_types[issue_type].append(issue['device'])
        
        for issue_type, devices in issue_types.items():
            print(f"{Fore.YELLOW}{issue_type}:")
            print(f"  Affected devices: {', '.join(devices)}")
            
            if issue_type == 'Unknown in Home Assistant':
                print(f"  {Fore.GREEN}Solution: Restart the MQTT bridge to re-publish states")
                print(f"  {Fore.GREEN}Or manually query state: Check periodic polling is enabled")
            elif issue_type == 'Missing MQTT State':
                print(f"  {Fore.GREEN}Solution: MQTT state not published during setup")
                print(f"  {Fore.GREEN}Fix: Restart MQTT bridge with fresh connection")
                print(f"  {Fore.YELLOW}Note: If MQTT bridge is running, check Home Assistant MQTT integration")
            elif issue_type == 'Zencontrol Query Failed':
                print(f"  {Fore.GREEN}Solution: Check DALI bus connection and device addressing")
                print(f"  {Fore.GREEN}Fix: Verify devices are powered and responsive")
            elif issue_type == 'Connection Failed':
                print(f"  {Fore.GREEN}Solution: Check MQTT broker connection settings")
                print(f"  {Fore.GREEN}Fix: Verify host, port, username, password in config")
                print(f"  {Fore.YELLOW}Note: MQTT bridge might still be working - check Home Assistant")
            print()

async def main():
    diagnostic = StatesDiagnostic()
    await diagnostic.run()

if __name__ == "__main__":
    asyncio.run(main())
