#!/usr/bin/env python3
"""
Bridge2 - Enhanced MQTT Bridge for Zencontrol

A comprehensive replacement for mqtt_bridge.py that:
1. Publishes all device states with retention on startup (fixing "Unknown" states)
2. Maintains continuous event handling for real-time synchronization
3. Includes all existing features (buttons, motion sensors, system variables, profiles)
4. Uses the existing config.yaml format

This bridge combines the diagnostic capabilities of diagnose_v2.py with the
continuous operation of mqtt_bridge.py.
"""

import sys
import os

# Auto-detect and use venv Python if dependencies are missing
try:
    import aiomqtt
    import colorama
    import yaml
    import zencontrol
except ImportError:
    # Dependencies not found, try using venv Python
    script_dir = os.path.dirname(os.path.abspath(__file__))
    venv_python = os.path.join(script_dir, 'venv', 'bin', 'python3')
    if os.path.exists(venv_python):
        os.execv(venv_python, [venv_python] + sys.argv)
    else:
        print("Error: Required dependencies (aiomqtt, colorama, yaml, zencontrol) not found.")
        print("Please install them in a virtual environment or install them system-wide.")
        sys.exit(1)

import asyncio
import ipaddress
import time
import json
import yaml
import re
from typing import Optional, Any
import zencontrol
from zencontrol import ZenController, ZenProtocol, ZenClient, ZenColour, ZenColourType, ZenProfile, ZenLight, ZenGroup, ZenButton, ZenMotionSensor, ZenSystemVariable, ZenTimeoutError, ZenAddressType
import aiomqtt
from colorama import Fore, Back, Style
import logging
from logging.handlers import RotatingFileHandler
import math
import pickle
import traceback

class ColoredConsoleFormatter(logging.Formatter):
    """Custom formatter with colors and better timestamps"""
    
    COLORS = {
        'DEBUG': Fore.WHITE + Style.DIM,
        'INFO': Fore.CYAN,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.RED + Style.BRIGHT,
    }
    
    def format(self, record):
        # Add color based on level
        color = self.COLORS.get(record.levelname, '')
        record.levelname = f"{color}{record.levelname}{Style.RESET_ALL}"
        return super().format(record)

class RateLimiter:
    """Rate limiter to control concurrent coroutine execution"""
    
    def __init__(self, max_concurrent: int = 5, delay_between_batches: float = 0.1):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.delay_between_batches = delay_between_batches
        self.last_batch_time = 0
        
    async def execute(self, coro):
        """Execute a coroutine with rate limiting"""
        # Ensure minimum delay between batches
        current_time = time.time()
        time_since_last_batch = current_time - self.last_batch_time
        if time_since_last_batch < self.delay_between_batches:
            await asyncio.sleep(self.delay_between_batches - time_since_last_batch)
        
        async with self.semaphore:
            self.last_batch_time = time.time()
            return await coro
    
    async def execute_batch(self, coros, batch_size: int = None):
        """Execute multiple coroutines in controlled batches"""
        if batch_size is None:
            batch_size = self.semaphore._value  # Use semaphore limit as batch size
            
        results = []
        for i in range(0, len(coros), batch_size):
            batch = coros[i:i + batch_size]
            batch_results = await asyncio.gather(*[self.execute(coro) for coro in batch])
            results.extend(batch_results)
            
        return results

class Const:
    STARTUP_POLL_DELAY = 10

    # MQTT settings
    MQTT_RECONNECT_MIN_DELAY = 1
    MQTT_RECONNECT_MAX_DELAY = 10
    MQTT_SERVICE_PREFIX = "zencontrol-python"
    
    # Logging
    LOG_FILE = 'mqtt.log'
    DEBUG_FILE = 'mqtt.debug.log'
    LOG_MAX_BYTES = 5 * 1024 * 1024  # 5MB
    LOG_BACKUP_COUNT = 5
    
    # Logarithmic constants
    # Based on curves seen in DALI documentation
    LOG_A = -59.53
    LOG_B = 56.58

    # Default hold time for motion sensors, in seconds
    DEFAULT_HOLD_TIME = 15

    # Default long press time for buttons, in msec
    DEFAULT_LONG_PRESS_TIME = 1000

class ZenMQTTBridge2:
    """Enhanced Bridge between Zen lighting control system and MQTT/Home Assistant.
    
    This bridge combines the diagnostic capabilities of diagnose_v2.py with the
    continuous operation of mqtt_bridge.py. It publishes all device states with
    retention on startup to fix "Unknown" states in Home Assistant.
    """
    
    # ================================
    #          INIT & RUN
    # ================================
    
    def __init__(self, config_path: str = "examples/config.yaml") -> None:
        self.config: dict[str, Any]
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        
        self.logger: logging.Logger
        self.discovery_prefix: str
        self.control: list[ZenController]
        self.zen: zencontrol.ZenControl
        self.mqttc: aiomqtt.Client
        self.setup_started: bool = False
        self.setup_complete: bool = False
        self.system_variables: list[ZenSystemVariable] = []
        self.control: list[ZenController] = []
        self.sv_config: list[dict] = []

        self.config_topics_to_delete: list[str] = [] # List of topics to delete after completing setup
        self.topic_object: dict[str, Any] = {} # Map of topics to objects
        
        # Rate limiter for controlling concurrent operations
        self.rate_limiter = RateLimiter(max_concurrent=5, delay_between_batches=0.1)

        self.global_config: dict[str, Any] = {
            "origin": {
                "name": "zencontrol-python",
                "sw": "0.0.0",
                "url": "https://github.com/sjwright/zencontrol-python"
            }
        }

    async def run(self) -> None:
        """Main bridge execution loop with enhanced startup state publishing"""
        self.setup_config()
        self.setup_logging()
        self.logger.info("==================================== Starting ZenMQTTBridge2 ====================================")
        await self.setup_zen()
        await self.setup_mqtt()
        
        # Wait for Zen controllers to be ready
        for ctrl in self.control:
            print(f"Connecting to Zen controller {ctrl.name} on {ctrl.host}:{ctrl.port}...")
            self.logger.info(f"Connecting to Zen controller {ctrl.name} on {ctrl.host}:{ctrl.port}...")

            try:
                while not await ctrl.is_controller_ready():
                    print(f"Controller {ctrl.label} still starting up...")
                    await asyncio.sleep(Const.STARTUP_POLL_DELAY)
            
            except ZenTimeoutError as e:
                self.logger.fatal(f"Aborting - Zen controller {ctrl.name} cannot be reached.")
                return # Don't reach the run loop
                
            except Exception as e:
                self.logger.fatal(f"Aborting - Error connecting to Zen controller {ctrl.name}: {e}")
                return # Don't reach the run loop

            # It's ready, interview it.
            await ctrl.interview()
        
        # Start MQTT message handling task
        self.logger.info("Starting MQTT message handler...")
        self.mqtt_task = asyncio.create_task(self._mqtt_message_handler())

        # Wait for MQTT connection to be established
        self.logger.info("Waiting for MQTT connection to be established...")
        await asyncio.sleep(2.0)

        # Generate config topics
        self.logger.info("Starting device setup...")
        self.setup_started = True
        await self.setup_profiles()
        self.logger.info("Profiles setup complete")
        await self.setup_lights()
        self.logger.info("Lights setup complete")
        await self.setup_groups()
        self.logger.info("Groups setup complete")
        await self.setup_buttons()
        self.logger.info("Buttons setup complete")
        await self.setup_motion_sensors()
        self.logger.info("Motion sensors setup complete")
        await self.setup_system_variables()
        self.logger.info("System variables setup complete")
        await self.delete_retained_topics()
        self.logger.info("Retained topics cleanup complete")
        self.setup_complete = True

        # Print device discovery summary
        self.logger.info("Printing device discovery summary...")
        await self._print_device_summary()

        # NEW: Query and publish ALL states with retain=True before starting event listeners
        self.logger.info("Starting initial state publishing...")
        await self.publish_all_initial_states()
        self.logger.info("Initial state publishing complete")

        # Begin listening for zen events
        self.logger.info("Starting Zen event listeners...")
        await self.zen.start()
        self.logger.info("Zen event listeners started")

        # Start periodic state polling task
        self.logger.info("Starting periodic state polling...")
        self.poll_task = asyncio.create_task(self._periodic_state_poll())
        self.logger.info("Periodic state polling started")

        self.logger.info("Saving cache...")
        with open("examples/cache.pkl", "wb") as f:
            pickle.dump(self.zen.cache, f)
        self.logger.info("Cache saved")
        
        clist = []
        for c in sorted(self.control, key=lambda x: x.id):
            clist.append(f"{c.label} ({c.host})")
        self.logger.info(f"Bridge2 ready - Controllers: {', '.join(clist)}")
        
        # Keep running
        self.logger.info("Bridge2 is now running - waiting for events...")
        try:
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            self.logger.info("Shutdown requested...")
            await self.stop()

    async def stop(self) -> None:
        """Graceful shutdown"""
        self.logger.info("Stopping bridge...")
        if hasattr(self, 'mqtt_task'):
            self.mqtt_task.cancel()
        if hasattr(self, 'poll_task'):
            self.poll_task.cancel()
        if hasattr(self, 'zen'):
            await self.zen.stop()
        self.logger.info("Bridge stopped")

    # ================================
    #       CONFIGURATION & SETUP
    # ================================

    def setup_config(self) -> None:
        """Setup configuration from config.yaml"""
        self.discovery_prefix = self.config['homeassistant']['discovery_prefix']
        
        # Parse system variables configuration
        self.sv_config = []
        for sv_config in self.config.get('system_variables', []):
            ctrl = next((c for c in self.control if c.name == sv_config['controller']), None)
            if ctrl:
                self.sv_config.append({
                    'controller': ctrl,
                    'id': sv_config['id'],
                    'object_id': sv_config['object_id'],
                    'component': sv_config['component'],
                    'attributes': sv_config.get('attributes', {})
                })

    def setup_logging(self) -> None:
        """Setup logging with colored console output"""
        self.logger = logging.getLogger('zencontrol')
        self.logger.setLevel(logging.DEBUG)
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # File handler
        file_handler = RotatingFileHandler(
            Const.LOG_FILE,
            maxBytes=Const.LOG_MAX_BYTES,
            backupCount=Const.LOG_BACKUP_COUNT
        )
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG)
        self.logger.addHandler(file_handler)
        
        # Console handler with colors
        console_handler = logging.StreamHandler()
        console_formatter = ColoredConsoleFormatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        
        # Set console level from config
        console_level = self.config.get('logging', {}).get('console_level', 'INFO')
        console_handler.setLevel(getattr(logging, console_level.upper()))
        self.logger.addHandler(console_handler)

    async def setup_zen(self) -> None:
        """Setup Zencontrol connection"""
        self.zen = zencontrol.ZenControl(
            print_traffic=self.config.get('logging', {}).get('show_mqtt_traffic', False),
            logger=self.logger
        )
        
        # Register Zen event callbacks
        self.zen.on_connect = self._zen_on_connect
        self.zen.on_disconnect = self._zen_on_disconnect
        self.zen.profile_change = self._zen_profile_change
        self.zen.group_change = self._zen_group_change
        self.zen.light_change = self._zen_light_change
        self.zen.button_press = self._zen_button_press
        self.zen.button_long_press = self._zen_button_long_press
        self.zen.motion_event = self._zen_motion_event
        self.zen.system_variable_change = self._zen_system_variable_change
        
        # Add controllers from config
        for ctrl_config in self.config.get('zencontrol', []):
            ctrl = self.zen.add_controller(
                id=ctrl_config['id'],
                name=ctrl_config['name'],
                label=ctrl_config['label'],
                host=ctrl_config['host'],
                port=ctrl_config.get('port', 5108),
                mac=ctrl_config['mac']
            )
            self.control.append(ctrl)

    async def setup_mqtt(self) -> None:
        """Setup MQTT connection"""
        mqtt_config = self.config['mqtt']
        self.mqttc = aiomqtt.Client(
            hostname=mqtt_config['host'],
            port=mqtt_config['port'],
            username=mqtt_config.get('user'),
            password=mqtt_config.get('password'),
            keepalive=60
        )

    # ================================
    #    INITIAL STATE PUBLISHING
    # ================================

    async def publish_all_initial_states(self) -> None:
        """Query and publish all device states with retention on startup"""
        self.logger.info("🔄 Publishing initial states with retention...")
        
        published_count = 0
        failed_count = 0
        
        # Query and publish all lights
        lights = await self.zen.get_lights()
        for light in lights:
            mqtt_topic = light.client_data.get("light", {}).get('mqtt_topic')
            if not mqtt_topic:
                failed_count += 1
                self.logger.warning(f"⚠️ Failed to publish initial state for {light.label}: No MQTT topic")
                continue
                
            try:
                level = await self.zen.protocol.dali_query_level(light.address)
                if level is not None:
                    state = {
                        "state": "OFF" if level == 0 else "ON",
                        "brightness": self.arc_to_brightness(level)
                    }
                    await self._publish_state(mqtt_topic, state, retain=True)
                    published_count += 1
                    self.logger.debug(f"✓ Published initial state for {light.label}: Level={level}")
                else:
                    # Query returned None, publish default state
                    failed_count += 1
                    default_state = {"state": "OFF", "brightness": 0}
                    await self._publish_state(mqtt_topic, default_state, retain=True)
                    self.logger.warning(f"⚠️ Failed to query {light.label}, published default OFF state")
            except Exception as e:
                # Query failed, publish default state so HA doesn't show "Unknown"
                failed_count += 1
                default_state = {"state": "OFF", "brightness": 0}
                await self._publish_state(mqtt_topic, default_state, retain=True)
                self.logger.warning(f"⚠️ Failed to query {light.label}: {e}, published default OFF state")
        
        # Query and publish all groups
        groups = await self.zen.get_groups()
        for group in groups:
            if not group.lights:
                continue
            mqtt_topic = group.client_data.get("light", {}).get('mqtt_topic')
            if not mqtt_topic:
                failed_count += 1
                self.logger.warning(f"⚠️ Failed to publish initial state for {group.label}: No MQTT topic")
                continue
                
            try:
                level = await self.zen.protocol.dali_query_level(group.address)
                if level is not None:
                    state = {
                        "state": "OFF" if level == 0 else "ON",
                        "brightness": self.arc_to_brightness(level)
                    }
                    await self._publish_state(mqtt_topic, state, retain=True)
                    published_count += 1
                    self.logger.debug(f"✓ Published initial state for {group.label}: Level={level}")
                else:
                    # Query returned None, publish default state
                    failed_count += 1
                    default_state = {"state": "OFF", "brightness": 0}
                    await self._publish_state(mqtt_topic, default_state, retain=True)
                    self.logger.warning(f"⚠️ Failed to query {group.label}, published default OFF state")
            except Exception as e:
                # Query failed, publish default state so HA doesn't show "Unknown"
                failed_count += 1
                default_state = {"state": "OFF", "brightness": 0}
                await self._publish_state(mqtt_topic, default_state, retain=True)
                self.logger.warning(f"⚠️ Failed to query {group.label}: {e}, published default OFF state")
        
        self.logger.info(f"✅ Initial state publishing complete: {published_count} published, {failed_count} failed")
        
        if failed_count > 0:
            self.logger.warning(f"⚠️ {failed_count} devices failed initial state publishing - they may show as 'Unknown' in Home Assistant")

    # ================================
    #        HELPER METHODS
    # ================================

    def _get_device_name(self, obj: Any) -> str:
        """Get human-readable device name, prioritizing Zencontrol labels"""
        if hasattr(obj, 'label') and obj.label:
            return obj.label
        elif hasattr(obj, 'name') and obj.name:
            return obj.name
        elif hasattr(obj, 'address'):
            return f"{obj.address.controller.name} {obj.address.type.name} {obj.address.number}"
        else:
            return str(obj)

    def arc_to_brightness(self, arc_level: int) -> int:
        """Convert DALI arc level (0-254) to Home Assistant brightness (0-255).

        Use a simple linear mapping to avoid non-intuitive rounding and to
        ensure consistency with Home Assistant expectations.
        """
        arc_level = max(0, min(254, int(arc_level)))
        # Linear scale: 0..254 -> 0..255
        return int(round((arc_level / 254) * 255))

    def brightness_to_arc(self, brightness: int) -> int:
        """Convert Home Assistant brightness (0-255) to DALI arc level (0-254).

        Use a simple linear mapping so that typical HA brightness values map
        directly to expected DALI arc levels (e.g., 255 -> 254, 128 -> 127).
        """
        brightness = max(0, min(255, int(brightness)))
        # Linear scale: 0..255 -> 0..254
        if brightness == 0:
            return 0
        return int(round((brightness / 255) * 254))

    def kelvin_to_mireds(self, kelvin: int) -> int:
        """Convert Kelvin to mireds for Home Assistant"""
        return int(1000000 / kelvin)

    def mireds_to_kelvin(self, mireds: int) -> int:
        """Convert mireds to Kelvin"""
        return int(1000000 / mireds)

    async def _publish_state(self, topic: str, state: str|dict|None, retain: bool = True) -> None:
        """Publish state to MQTT with retention by default"""
        try:
            if isinstance(state, dict): 
                state = json.dumps(state)
            await self.mqttc.publish(f"{topic}/state", state, retain=retain)
            
            # Log state publishes for lights/groups with verification
            if "/light/" in topic or "/group/" in topic:
                self.logger.debug(f"📡 MQTT Publish → {topic}/state: {state} (retained={retain})")
        except Exception as e:
            self.logger.error(f"❌ MQTT Publish failed for {topic}: {e}")

    async def _publish_config(self, topic: str, config: dict, object: Any = None) -> None:
        """Publish Home Assistant auto-discovery config"""
        try:
            config_json = json.dumps(config)
            await self.mqttc.publish(f"{topic}/config", config_json, retain=True)
            
            if object:
                self.topic_object[topic] = object
        except Exception as e:
            self.logger.error(f"❌ MQTT Config publish failed for {topic}: {e}")

    def _client_data_for_object(self, object: Any, component: str, attributes: dict = {}) -> dict:
        """Generate client data for Home Assistant auto-discovery"""
        if isinstance(object, ZenController):
            ctrl = object
            serial = ctrl.mac
            mqtt_target = "profile"
        elif isinstance(object, ZenGroup):  # ZenGroup inherits from ZenLight, so needs to be before ZenLight
            group = object
            addr = group.address
            ctrl = addr.controller
            serial = ""
            mqtt_target = f"group{addr.number}"
        elif isinstance(object, ZenLight):
            light = object
            addr = light.address
            ctrl = addr.controller
            serial = light.serial
            mqtt_target = f"ecg{addr.number}"
        elif isinstance(object, ZenButton):
            button = object
            inst = button.instance
            addr = inst.address
            ctrl = addr.controller
            serial = button.serial
            mqtt_target = f"ecd{addr.number}_inst{inst.number}"
        elif isinstance(object, ZenMotionSensor):
            sensor = object
            inst = sensor.instance
            addr = inst.address
            ctrl = addr.controller
            serial = sensor.serial
            mqtt_target = f"ecd{addr.number}_inst{inst.number}"
        elif isinstance(object, ZenSystemVariable):
            sysvar = object
            ctrl = sysvar.controller
            serial = component
            mqtt_target = f"sv{sysvar.id}"
        else:
            raise ValueError(f"Unknown object type: {type(object)}")
        object.client_data[component] = object.client_data.get(component, {}) | {
            "component": component,
            "attributes": attributes | {
                "component": component,
                "object_id": f"{ctrl.name}_{mqtt_target}",
                "unique_id": f"{ctrl.name}_{mqtt_target}_{serial}",
                "device": {
                    "manufacturer": "Zencontrol",
                    "identifiers": f"zencontrol-{ctrl.name}",
                    "sw_version": ctrl.version,
                    "name": ctrl.label,
                },
                "availability_topic": f"{Const.MQTT_SERVICE_PREFIX}/{ctrl.name}/availability",
            },
            "mqtt_target": mqtt_target,
            "mqtt_topic": f"{self.discovery_prefix}/{component}/{ctrl.name}/{mqtt_target}",
        }
        return object.client_data[component]

    async def _print_device_summary(self) -> None:
        """Print summary of discovered devices"""
        lights = await self.zen.get_lights()
        groups = await self.zen.get_groups()
        buttons = await self.zen.get_buttons()
        motion_sensors = await self.zen.get_motion_sensors()
        profiles = await self.zen.get_profiles()
        
        self.logger.info(f"📊 Device Summary:")
        self.logger.info(f"   💡 Lights: {len(lights)}")
        self.logger.info(f"   🏠 Groups: {len(groups)}")
        self.logger.info(f"   🔘 Buttons: {len(buttons)}")
        self.logger.info(f"   👁️ Motion Sensors: {len(motion_sensors)}")
        self.logger.info(f"   🎛️ Profiles: {len(profiles)}")
        self.logger.info(f"   📊 System Variables: {len(self.system_variables)}")

    # ================================
    #        PLACEHOLDER METHODS
    # ================================
    # These will be implemented in subsequent phases

    async def setup_profiles(self) -> set[ZenProfile]:
        """Initialize all profiles for Home Assistant auto-discovery."""
        all_profiles = set()
        for ctrl in self.control:
            client_data = self._client_data_for_object(ctrl, "select")
            mqtt_topic = client_data['mqtt_topic']
            profiles = await self.zen.get_profiles(ctrl)
            config_dict = self.global_config | client_data.get("attributes",{}) | {
                "name": f"{ctrl.label} Profile",
                "command_topic": f"{mqtt_topic}/set",
                "state_topic": f"{mqtt_topic}/state",
                "options": [
                    profile.label for profile in profiles
                ]
            }
            await self._publish_config(mqtt_topic, config_dict, object=ctrl)
            await self._publish_state(mqtt_topic, ctrl.profile.label)
            all_profiles.update(profiles)
        return all_profiles

    async def setup_lights(self) -> set[ZenLight]:
        """Initialize all lights for Home Assistant auto-discovery."""
        lights = await self.zen.get_lights()
        
        failed_queries = []
        
        # First, publish all configs (this is fast and doesn't need rate limiting)
        for light in lights:
            client_data = self._client_data_for_object(light, "light")
            mqtt_topic = client_data['mqtt_topic']
            config_dict = self.global_config | client_data.get("attributes",{}) | {
                "name": light.label,
                "schema": "json",
                "payload_off": "OFF",
                "payload_on": "ON",
                "command_topic": f"{mqtt_topic}/set",
                "state_topic": f"{mqtt_topic}/state",
                "json_attributes_topic": f"{mqtt_topic}/attributes",
                "effect": False,
                "retain": False,
                "brightness": light.features["brightness"],
                "supported_color_modes": [],
            }
            if light.features["RGBWW"]:
                config_dict["supported_color_modes"] = ["rgbww"]
            elif light.features["RGBW"]:
                config_dict["supported_color_modes"] = ["rgbw"]
            elif light.features["RGB"]:
                config_dict["supported_color_modes"] = ["rgb"]
            elif light.features["temperature"]:
                config_dict["supported_color_modes"] = ["color_temp"]
                config_dict["min_mireds"] = self.kelvin_to_mireds(light.properties["max_kelvin"]) if light.properties["max_kelvin"] is not None else None
                config_dict["max_mireds"] = self.kelvin_to_mireds(light.properties["min_kelvin"]) if light.properties["min_kelvin"] is not None else None
            elif light.features["brightness"]:
                config_dict["supported_color_modes"] = ["brightness"]
            else:
                config_dict["supported_color_modes"] = ["onoff"]

            await self._publish_config(mqtt_topic, config_dict, object=light)

        # Then, sync all lights with rate limiting to prevent server overload
        sync_coros = []
        for light in lights:
            # Verify state can be queried during setup
            try:
                level = await self.zen.protocol.dali_query_level(light.address)
                if level is None or level == 255:
                    self.logger.warning(f"⚠️ {self._get_device_name(light)}: Initial state query returned invalid value ({level})")
                    failed_queries.append(light.label)
                    # Set a default state
                    level = 0
                
                # Publish initial state
                new_state = {
                    "state": "OFF" if level == 0 else "ON",
                    "brightness": self.arc_to_brightness(level)
                }
                await self._publish_state(mqtt_topic, new_state)
                self.logger.debug(f"✓ {self._get_device_name(light)}: Initial state published (level={level})")
            
            except Exception as e:
                self.logger.error(f"❌ {self._get_device_name(light)}: Failed to query initial state - {e}")
                failed_queries.append(light.label)
                # Publish a safe default
                await self._publish_state(mqtt_topic, {"state": "OFF", "brightness": 0})
            
            # Add to sync coros for normal refresh
            sync_coros.append(light.refresh_state_from_controller())
        
        await self.rate_limiter.execute_batch(sync_coros)
        
        if failed_queries:
            self.logger.warning(f"⚠️ {len(failed_queries)} devices failed initial state query: {', '.join(failed_queries)}")
            self.logger.warning(f"⚠️ These devices may show as 'Unknown' in Home Assistant until they change state")

        # Return all lights
        return lights

    async def setup_groups(self) -> set[ZenGroup]:
        """Initialize all groups for Home Assistant auto-discovery."""
        groups = await self.zen.get_groups()
        failed_queries = []
        
        # Group-lights
        for group in groups:
            client_data = self._client_data_for_object(group, "light")
            mqtt_topic = client_data['mqtt_topic']
            config_dict = self.global_config | client_data.get("attributes",{}) | {
                "name": group.label,
                "schema": "json",
                "payload_off": "OFF",
                "payload_on": "ON",
                "command_topic": f"{mqtt_topic}/set",
                "state_topic": f"{mqtt_topic}/state",
                "json_attributes_topic": f"{mqtt_topic}/attributes",
                "effect": False,
                "retain": False,
                "brightness": False,
            }
            if group.contains_temperature_lights():
                config_dict = config_dict | {
                    "brightness": True,
                    "supported_color_modes": ["color_temp"],
                    "min_mireds": self.kelvin_to_mireds(group.properties["max_kelvin"]),
                    "max_mireds": self.kelvin_to_mireds(group.properties["min_kelvin"]),
                }
            elif group.contains_dimmable_lights():
                config_dict = config_dict | {
                    "brightness": True,
                    "supported_color_modes": ["brightness"],
                }
            else:
                config_dict = config_dict | {
                    "supported_color_modes": ["onoff"],
                }
            await self._publish_config(mqtt_topic, config_dict, object=group)
            
            # Verify state can be queried during setup
            try:
                level = await self.zen.protocol.dali_query_level(group.address)
                if level is None or level == 255:
                    self.logger.warning(f"⚠️ {self._get_device_name(group)}: Initial state query returned invalid value ({level})")
                    failed_queries.append(group.label)
                    # Set a default state
                    level = 0
                
                # Publish initial state
                new_state = {
                    "state": "OFF" if level == 0 else "ON",
                    "brightness": self.arc_to_brightness(level)
                }
                await self._publish_state(mqtt_topic, new_state)
                self.logger.debug(f"✓ {self._get_device_name(group)}: Initial state published (level={level})")
            
            except Exception as e:
                self.logger.error(f"❌ {self._get_device_name(group)}: Failed to query initial state - {e}")
                failed_queries.append(group.label)
                # Publish a safe default
                await self._publish_state(mqtt_topic, {"state": "OFF", "brightness": 0})
            
            # Get the latest state from the controller and trigger an event, which then sends a state update
            await group.refresh_state_from_controller()

        # Group-scenes
        for group in groups:
            client_data = self._client_data_for_object(group, "select")
            mqtt_topic = client_data['mqtt_topic']
            config_dict = self.global_config | client_data.get("attributes",{}) | {
                "name": group.label,
                "command_topic": f"{mqtt_topic}/set",
                "state_topic": f"{mqtt_topic}/state",
                "options": group.get_scene_labels(exclude_none=True),
            }
            await self._publish_config(mqtt_topic, config_dict, object=group)
            await self._publish_state(mqtt_topic, group.scene)

        if failed_queries:
            self.logger.warning(f"⚠️ {len(failed_queries)} groups failed initial state query: {', '.join(failed_queries)}")
            self.logger.warning(f"⚠️ These groups may show as 'Unknown' in Home Assistant until they change state")

        # Return all groups
        return groups

    async def setup_buttons(self) -> set[ZenButton]:
        """Initialize all buttons found on the DALI bus for Home Assistant auto-discovery."""
        buttons = await self.zen.get_buttons()
        for button in buttons:
            client_data = self._client_data_for_object(button, "device_automation")
            button.long_press_time = Const.DEFAULT_LONG_PRESS_TIME
            mqtt_topic = client_data['mqtt_topic']
            config_dict = self.global_config | client_data.get("attributes",{}) | {
                "automation_type": "trigger",
                "subtype": re.sub(r'[^a-z0-9]', '_', button.label.lower()) + "_" + re.sub(r'[^a-z0-9]', '_', button.instance_label.lower()),
                "type": "button_short_press",
                "payload": "button_short_press",
                "topic": f"{mqtt_topic}/event",
            }
            await self._publish_config(mqtt_topic, config_dict, object=button)
            # For long press, we use a different topic for the config, but the same topic for the event payload
            config_dict = config_dict | {
                "type": "button_long_press",
                "payload": "button_long_press",
            }
            await self._publish_config(mqtt_topic + "_long_press", config_dict, object=button)
        
        # Return all buttons
        return buttons

    async def setup_motion_sensors(self) -> set[ZenMotionSensor]:
        """Initialize all motion sensors found on the DALI bus for Home Assistant auto-discovery."""
        sensors = await self.zen.get_motion_sensors()
        for sensor in sensors:
            client_data = self._client_data_for_object(sensor, "binary_sensor")
            sensor.hold_time = Const.DEFAULT_HOLD_TIME
            mqtt_topic = client_data['mqtt_topic']
            config_dict = self.global_config | client_data.get("attributes",{}) | {
                "name": sensor.instance_label,
                "device_class": "motion",
                "payload_off": "OFF",
                "payload_on": "ON",
                "state_topic": f"{mqtt_topic}/state",
                "json_attributes_topic": f"{mqtt_topic}/attributes",
                "retain": False,
            }
            await self._publish_config(mqtt_topic, config_dict, object=sensor)
            await self._publish_state(mqtt_topic, "ON" if sensor.occupied else "OFF")

        # Return all motion sensors
        return sensors

    async def setup_system_variables(self) -> set[ZenSystemVariable]:
        """Initialize system variables in config.yaml for Home Assistant auto-discovery."""
        
        # On first run, prep system variables with client_data
        if not self.system_variables:
            for sv in self.sv_config:
                ctrl: ZenController = sv['controller']
                zsv = ctrl.get_sysvar(sv['id'])
                attr = sv['attributes'] | {
                    "object_id": sv['object_id'],
                    "unique_id": f"{ctrl.name}_{sv['object_id']}"
                }
                self._client_data_for_object(zsv, sv['component'], attributes=attr)
                self.system_variables.append(zsv)

        for zsv in self.system_variables:
            if zsv.client_data.get("switch", None):
                client_data = zsv.client_data["switch"]
                mqtt_topic = client_data["mqtt_topic"]
                config_dict = self.global_config | client_data.get("attributes",{}) | {
                    "component": "switch",
                    "state_topic": f"{mqtt_topic}/state",
                    "command_topic": f"{mqtt_topic}/set",
                    "payload_off": "OFF",
                    "payload_on": "ON",
                    "retain": False,
                }
            elif zsv.client_data.get("sensor", None):
                client_data = zsv.client_data["sensor"]
                mqtt_topic = client_data["mqtt_topic"]
                config_dict = self.global_config | client_data.get("attributes",{}) | {
                    "component": "sensor",
                    "state_topic": f"{mqtt_topic}/state",
                    "retain": False,
                }
            else:
                continue

            await self._publish_config(mqtt_topic, config_dict, object=zsv)
            await self._publish_state(mqtt_topic, await zsv.get_value())

        # Return all system variables
        return self.system_variables

    async def delete_retained_topics(self) -> None:
        """Delete retained topics that are no longer needed"""
        for topic in self.config_topics_to_delete:
            await self.mqttc.publish(topic, "", retain=True)

    async def _mqtt_message_handler(self) -> None:
        """Handle incoming MQTT messages with reconnection logic"""
        self.logger.info("MQTT message handler starting...")
        interval = Const.MQTT_RECONNECT_MIN_DELAY
        
        while True:
            try:
                self.logger.info("Attempting MQTT connection...")
                # Use the client context manager for automatic connection handling
                async with self.mqttc:
                    self.logger.info("MQTT connected, subscribing to topics...")
                    # Subscribe to topics
                    for ctrl in self.control:
                        await self.mqttc.subscribe(f"{self.discovery_prefix}/light/{ctrl.name}/#")
                        await self.mqttc.subscribe(f"{self.discovery_prefix}/binary_sensor/{ctrl.name}/#")
                        await self.mqttc.subscribe(f"{self.discovery_prefix}/sensor/{ctrl.name}/#")
                        await self.mqttc.subscribe(f"{self.discovery_prefix}/switch/{ctrl.name}/#")
                        await self.mqttc.subscribe(f"{self.discovery_prefix}/event/{ctrl.name}/#")
                        await self.mqttc.subscribe(f"{self.discovery_prefix}/select/{ctrl.name}/#")
                        await self.mqttc.subscribe(f"{self.discovery_prefix}/device_automation/{ctrl.name}/#")
                        await self.mqttc.publish(f"{Const.MQTT_SERVICE_PREFIX}/{ctrl.name}/availability", "online", retain=True)
                    
                    self.logger.info("Successfully connected to MQTT broker")
                    
                    # Process messages
                    self.logger.info("Starting message processing loop...")
                    async for message in self.mqttc.messages:
                        await self._mqtt_on_message(message)
                        
            except asyncio.CancelledError:
                self.logger.info("MQTT message handler cancelled")
                break
            except aiomqtt.MqttError as e:
                self.logger.warning(f"MQTT connection lost: {e}")
                self.logger.info(f"Reconnecting in {interval} seconds...")
                await asyncio.sleep(interval)
                # Reset interval for next attempt
                interval = min(interval * 2, Const.MQTT_RECONNECT_MAX_DELAY)
            except Exception as e:
                self.logger.error(f"Unexpected error in MQTT handler: {e}")
                await asyncio.sleep(interval)

    async def _mqtt_on_message(self, message) -> None:
        """Handle individual MQTT messages"""
        topic = str(message.topic)
        payload = message.payload.decode() if message.payload else ""
        
        # Debug logging for all MQTT messages
        self.logger.debug(f"📨 MQTT Message received: {topic} = {payload}")
        
        # Get the last part of the topic
        command = topic.split('/')[-1]
        
        # Config commands are ignored
        if command == "config":
            return
        
        # State commands are always ignored
        if command == "state":
            return
        
        # Only set commands from here onwards
        if command != "set":
            return
        
        # Get the base topic from the message
        base_topic = topic.rsplit('/', 1)[0]
        
        # Find the matching object in our map
        target_object = self.topic_object.get(base_topic)
        
        # If we don't have an object, ignore the message
        if not target_object:
            self.logger.debug(f"No matching object found for {base_topic}")
            return
        
        # Parse topic to find component type
        parts = topic.split('/')
        if len(parts) >= 4:
            component = parts[1]
            
            self.logger.debug(f"🔍 Processing {component} command for {self._get_device_name(target_object)}")
            
            if component == "light" and parts[-1] == "set":
                self.logger.debug(f"💡 Processing light command for {self._get_device_name(target_object)}")
                await self._mqtt_light_change(target_object, json.loads(payload))
            elif component == "select" and parts[-1] == "set":
                if isinstance(target_object, ZenController):
                    self.logger.debug(f"🎛️ Processing profile command for {self._get_device_name(target_object)}")
                    await self._mqtt_profile_change(target_object, payload)
                elif isinstance(target_object, ZenGroup):
                    self.logger.debug(f"🏠 Processing group scene command for {self._get_device_name(target_object)}")
                    await self._mqtt_groupscene_change(target_object, payload)
            elif component == "switch" and parts[-1] == "set":
                self.logger.debug(f"🔌 Processing switch command for {self._get_device_name(target_object)}")
                await self._mqtt_system_variable_change(target_object, payload)
        else:
            self.logger.warning(f"❌ Invalid MQTT topic format: {topic}")

    async def _periodic_state_poll(self) -> None:
        """Periodic state polling task with mismatch detection and state correction"""
        poll_interval = self.config.get('state_sync', {}).get('poll_interval', 0)
        if poll_interval <= 0:
            self.logger.info("Periodic state polling disabled")
            return
        
        self.logger.info(f"Starting periodic state polling every {poll_interval} seconds")
        mismatch_count = 0
        total_polls = 0
        
        while True:
            try:
                await asyncio.sleep(poll_interval)
                total_polls += 1
                
                # Poll all lights and groups
                lights = await self.zen.get_lights()
                groups = await self.zen.get_groups()
                
                # Refresh states with rate limiting
                refresh_coros = []
                for light in lights:
                    refresh_coros.append(light.refresh_state_from_controller(verifying=True))
                for group in groups:
                    refresh_coros.append(group.refresh_state_from_controller(verifying=True))
                
                await self.rate_limiter.execute_batch(refresh_coros)
                
                # Check for state mismatches and publish corrections
                corrected_count = 0
                for light in lights:
                    mqtt_topic = light.client_data.get("light", {}).get('mqtt_topic')
                    if mqtt_topic and light.level is not None:
                        # Publish current state to ensure HA is in sync
                        state = {
                            "state": "OFF" if light.level == 0 else "ON",
                            "brightness": self.arc_to_brightness(light.level)
                        }
                        await self._publish_state(mqtt_topic, state, retain=True)
                        corrected_count += 1
                
                for group in groups:
                    mqtt_topic = group.client_data.get("light", {}).get('mqtt_topic')
                    if mqtt_topic and group.level is not None:
                        # Publish current state to ensure HA is in sync
                        state = {
                            "state": "OFF" if group.level == 0 else "ON",
                            "brightness": self.arc_to_brightness(group.level)
                        }
                        await self._publish_state(mqtt_topic, state, retain=True)
                        corrected_count += 1
                
                # Log periodic summary every 10 polls
                if total_polls % 10 == 0:
                    self.logger.info(f"📊 Periodic poll #{total_polls}: {len(lights)} lights, {len(groups)} groups checked, {corrected_count} states published")
                
            except Exception as e:
                self.logger.error(f"❌ Periodic state poll error: {e}")
                # Continue polling even if there's an error

    async def verify_all_states(self) -> None:
        """Manually verify all device states and report mismatches"""
        self.logger.info("🔍 Starting manual state verification...")
        
        lights = await self.zen.get_lights()
        groups = await self.zen.get_groups()
        
        # Verify all states with rate limiting
        verify_coros = []
        for light in lights:
            verify_coros.append(light.refresh_state_from_controller(verifying=True))
        for group in groups:
            verify_coros.append(group.refresh_state_from_controller(verifying=True))
        
        await self.rate_limiter.execute_batch(verify_coros)
        
        # Publish corrected states
        corrected_count = 0
        for light in lights:
            mqtt_topic = light.client_data.get("light", {}).get('mqtt_topic')
            if mqtt_topic and light.level is not None:
                state = {
                    "state": "OFF" if light.level == 0 else "ON",
                    "brightness": self.arc_to_brightness(light.level)
                }
                await self._publish_state(mqtt_topic, state, retain=True)
                corrected_count += 1
        
        for group in groups:
            mqtt_topic = group.client_data.get("light", {}).get('mqtt_topic')
            if mqtt_topic and group.level is not None:
                state = {
                    "state": "OFF" if group.level == 0 else "ON",
                    "brightness": self.arc_to_brightness(group.level)
                }
                await self._publish_state(mqtt_topic, state, retain=True)
                corrected_count += 1
        
        self.logger.info(f"✅ Manual verification complete: {len(lights)} lights, {len(groups)} groups checked, {corrected_count} states published")

    # ================================
    #        EVENT HANDLERS
    # ================================

    async def _mqtt_light_change(self, light: ZenLight|ZenGroup, payload: dict[str, Any]) -> None:
        """Handle MQTT light commands from Home Assistant"""
        self.logger.info(f"🔧 _mqtt_light_change called for {self._get_device_name(light)} with payload: {payload}")
        
        addr = light.address
        ctrl = addr.controller
        state: Optional[str] = payload.get("state", None)
        brightness: Optional[int] = payload.get("brightness", None)
        mireds: Optional[int] = payload.get("color_temp", None)

        self.logger.info(f"🔍 Parsed command: state={state}, brightness={brightness}, mireds={mireds}")

        # If brightness or temperature is set
        if brightness or mireds:
            args = {}
            if brightness: 
                args["level"] = self.brightness_to_arc(brightness)
                self.logger.info(f"💡 Setting brightness: {brightness} -> arc level {args['level']}")
            if mireds: 
                args["colour"] = ZenColour(type=ZenColourType.TC, kelvin=self.mireds_to_kelvin(mireds))
                self.logger.info(f"🌡️ Setting color temp: {mireds} mireds -> {self.mireds_to_kelvin(mireds)}K")
            
            self.logger.info(f"📥 HA Command → {self._get_device_name(light)}: Set {args}")
            try:
                await light.set(**args)
                self.logger.info(f"✅ Successfully executed light.set({args})")
            except Exception as e:
                self.logger.error(f"❌ Error executing light.set({args}): {e}")
            return
        
        # If switched on/off in HA
        if state == "OFF":
            self.logger.info(f"📥 HA Command → {self._get_device_name(light)}: Turn OFF")
            try:
                await light.off(fade=True)
                self.logger.info(f"✅ Successfully executed light.off()")
            except Exception as e:
                self.logger.error(f"❌ Error executing light.off(): {e}")
        elif state == "ON":
            self.logger.info(f"📥 HA Command → {self._get_device_name(light)}: Turn ON")
            try:
                await light.on()
                self.logger.info(f"✅ Successfully executed light.on()")
            except Exception as e:
                self.logger.error(f"❌ Error executing light.on(): {e}")
        else:
            self.logger.warning(f"⚠️ Unknown state command: {state}")

    async def _zen_on_connect(self) -> None:
        """Handle Zen controller connection"""
        self.logger.info("Connected to Zen controllers")

    async def _zen_on_disconnect(self) -> None:
        """Handle Zen controller disconnection"""
        self.logger.info("Disconnected from Zen controllers")

    async def _zen_light_change(self, light: ZenLight, level: Optional[int] = None, colour: Optional[ZenColour] = None, scene: Optional[int] = None) -> None:
        """Handle Zen light change events"""
        # Enhanced logging for state sync debugging
        state_transition = ""
        if level is not None:
            if light.level == 0 and level == 0:
                # Don't log OFF→OFF during normal operation
                state_transition = ""
            elif light.level != 0 and level == 0:
                state_transition = " (ON→OFF)"
            elif light.level == 0 and level != 0:
                state_transition = " (OFF→ON)"
        
        # Format the event details
        event_details = []
        if level is not None:
            event_details.append(f"Level={level}")
        if colour is not None:
            event_details.append(f"Color={colour}")
        if scene is not None:
            event_details.append(f"Scene={scene}")
        
        details_str = " ".join(event_details) if event_details else "No changes"
        
        self.logger.info(f"📤 Zen Event → {self._get_device_name(light)}: {details_str}{state_transition}")

        mqtt_topic = light.client_data.get("light", {}).get('mqtt_topic', None)
        if not mqtt_topic:
            self.logger.error(f"❌ Configuration Error → {self._get_device_name(light)}: No MQTT topic configured")
            return

        # If only color changed (no level), use current level to ensure state is published
        current_level = level if level is not None else light.level

        new_state = {
            "state": "OFF" if current_level == 0 else "ON"
        }

        # Always include brightness for clarity, even when OFF
        if current_level is not None:
            new_state["brightness"] = self.arc_to_brightness(current_level)

        if light.colour and light.colour.type == ZenColourType.TC:
            new_state["color_mode"] = "color_temp"
            if light.colour.kelvin is not None:
                new_state["color_temp"] = self.kelvin_to_mireds(light.colour.kelvin)

        await self._publish_state(mqtt_topic, new_state)

    async def _mqtt_groupscene_change(self, group: ZenGroup, payload: str) -> None:
        """Handle MQTT group scene commands from Home Assistant"""
        self.logger.info(f"📥 HA Command → {self._get_device_name(group)}: Set Scene '{payload}'")
        await group.set_scene(payload)

    async def _zen_group_change(self, group: ZenGroup, level: Optional[int] = None, colour: Optional[ZenColour] = None, scene: Optional[int] = None, discoordinated: Optional[bool] = None) -> None:
        """Handle Zen group change events"""
        select_mqtt_topic = group.client_data.get("select", {}).get('mqtt_topic', None)

        # Get the scene label for the ID from the group
        if select_mqtt_topic and scene is not None:
            scene_label = group.get_scene_label_from_number(scene)
            if scene_label:
                await self._publish_state(select_mqtt_topic, scene_label)
            else:
                await self._publish_state(select_mqtt_topic, "None")
                self.logger.warning(f"⚠️ Configuration Warning → {self._get_device_name(group)}: No scene with ID {scene}")
        
        # If discoordinated, set the group-scene to "None" but still publish the light state
        if discoordinated:
            self.logger.info(f"📤 Zen Event → {self._get_device_name(group)}: Discoordinated")
            await self._publish_state(select_mqtt_topic, "None")
            
            # Query actual level if not provided, so we can publish correct state to HA
            if level is None:
                level = await self.zen.protocol.dali_query_level(group.address)
                self.logger.debug(f"🔍 Queried discoordinated group level: {level}")
            
            # Continue to publish the actual light state below (don't return early)
        
        # Do light stuff (this will now run even if discoordinated)
        await self._zen_light_change(light=group, level=level, colour=colour, scene=scene)

    async def _mqtt_profile_change(self, ctrl: ZenController, payload: str) -> None:
        """Handle MQTT profile commands from Home Assistant"""
        self.logger.info(f"📥 HA Command → {self._get_device_name(ctrl)}: Switch to Profile '{payload}'")
        await ctrl.switch_to_profile(payload)

    async def _zen_profile_change(self, profile: ZenProfile) -> None:
        """Handle Zen profile change events"""
        self.logger.info(f"📤 Zen Event → {self._get_device_name(profile.controller)}: Profile changed to '{profile.label}'")

        ctrl = profile.controller
        mqtt_topic = ctrl.client_data.get("select", {}).get('mqtt_topic', None)
        if not mqtt_topic:
            self.logger.error(f"❌ Configuration Error → {self._get_device_name(ctrl)}: No MQTT topic")
            return

        await self._publish_state(mqtt_topic, profile.label)

    async def _zen_button_press(self, button: ZenButton) -> None:
        """Handle Zen button press events"""
        self.logger.debug(f"📤 Zen Event → {self._get_device_name(button)}: Button Press")
        mqtt_topic = button.client_data.get("device_automation", {}).get("mqtt_topic", None)
        if not mqtt_topic:
            self.logger.error(f"❌ Configuration Error → {self._get_device_name(button)}: No MQTT topic configured")
            return
        await self._publish_event(mqtt_topic, "button_short_press")
        
    async def _zen_button_long_press(self, button: ZenButton) -> None:
        """Handle Zen button long press events"""
        self.logger.debug(f"📤 Zen Event → {self._get_device_name(button)}: Button Long Press")
        mqtt_topic = button.client_data.get("device_automation", {}).get("mqtt_topic", None)
        if not mqtt_topic:
            self.logger.error(f"❌ Configuration Error → {self._get_device_name(button)}: No MQTT topic configured")
            return
        await self._publish_event(mqtt_topic, "button_long_press")

    async def _zen_motion_event(self, sensor: ZenMotionSensor, occupied: bool) -> None:
        """Handle Zen motion sensor events"""
        self.logger.debug(f"📤 Zen Event → {self._get_device_name(sensor)}: Motion {'Detected' if occupied else 'Cleared'}")
        mqtt_topic = sensor.client_data.get("binary_sensor", {}).get("mqtt_topic", None)
        if not mqtt_topic:
            self.logger.error(f"❌ Configuration Error → {self._get_device_name(sensor)}: No MQTT topic configured")
            return
        await self._publish_state(mqtt_topic, "ON" if occupied else "OFF")

    async def _mqtt_system_variable_change(self, sysvar: ZenSystemVariable, payload: str) -> None:
        """Handle MQTT system variable commands from Home Assistant"""
        self.logger.info(f"📥 HA Command → {self._get_device_name(sysvar)}: Set Value '{payload}'")
        if sysvar.client_data.get("switch", None):
            await sysvar.set_value(1 if payload == "ON" else 0)
        elif sysvar.client_data.get("sensor", None):
            return # Read only

    async def _zen_system_variable_change(self, system_variable: ZenSystemVariable, value:int, changed: bool, by_me: bool) -> None:
        """Handle Zen system variable change events"""
        self.logger.debug(f"📤 Zen Event → {self._get_device_name(system_variable)}: Value changed to {value}")
        if system_variable.client_data.get("switch", None):
            mqtt_topic = system_variable.client_data["switch"]["mqtt_topic"]
            await self._publish_state(mqtt_topic, "OFF" if value == 0 else "ON")
        elif system_variable.client_data.get("sensor", None):
            mqtt_topic = system_variable.client_data["sensor"]["mqtt_topic"]
            await self._publish_state(mqtt_topic, value)
        else:
            self.logger.error(f"❌ Configuration Error → {self._get_device_name(system_variable)}: No MQTT topic configured")
        return

    async def _publish_event(self, topic: str, event: str) -> None:
        """Publish device automation event"""
        try:
            await self.mqttc.publish(f"{topic}/event", event, retain=False)
        except Exception as e:
            self.logger.error(f"❌ MQTT Event publish failed for {topic}: {e}")

# Usage
async def main():
    bridge = ZenMQTTBridge2()
    try:
        await bridge.run()
    except KeyboardInterrupt:
        print("\nShutdown requested by user...")
        await bridge.stop()
    except Exception as e:
        print(f"Unexpected error: {e}")
        await bridge.stop()
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nGraceful shutdown complete.")
