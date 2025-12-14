import logging
import math
import numpy as np
import statistics
import time
from datetime import datetime, timedelta
from garmin_fit_sdk import Decoder, Stream, Profile
from typing import Generator

from .geo import geo_distance


SEMICIRCLES_FACTOR = 180.0 / 2**31
SMOOTH_ALTITUDE_TIME_WINDOW = 5 # seconds
MAX_GRADE_WINDOW = 50 # meters
MIN_GRADE_WINDOW = 20 # meters


units = {
    'time':                             's',
    'timestamp':                        None,
    'position_lat':                     '°',
    'position_long':                    '°',
    'altitude':                         'm',
    'smooth_altitude':                  'm',
    'heart_rate':                       'bpm',
    'cadence':                          'rpm',
    'distance':                         'm',
    'track_distance':                   'm',
    'speed':                            'm/s',
    'track_speed':                      'm/s',
    'power':                            'W',
    'power3s':                          'W',
    'power10s':                         'W',
    'power30s':                         'W',
    'grade':                            '%',
    'temperature':                      '°C',
    'accumulated_power':                'W',
    'left_right_balance':               None,
    'gps_accuracy':                     'm',
    'vertical_speed':                   'm/s',
    'calories':                         'kcal',
    'left_torque_effectiveness':        '%',
    'right_torque_effectiveness':       '%',
    'left_pedal_smoothness':            '%',
    'right_pedal_smoothness':           '%',
    'combined_pedal_smoothness':        '%',
    'respiration_rate':                 'bpm',
    'grit':                             None,
    'flow':                             None,
    'core_temperature':                 '°C',
    'front_gear_num':                   None,
    'front_gear':                       'teeth',
    'rear_gear_num':                    None,
    'rear_gear':                        'teeth',
    'active_climb':                     None, #experimental
}


class Reader:
    def __init__(self, fit_file: str):
        self.fit_file: str = fit_file
        self._data: dict[datetime, dict] = {}
        self._cache: dict = {}

        self.ok: bool = self._load_fit_file()
        if self.ok:
            self._generate_calculated_fields()


    @property
    def data(self) -> Generator[tuple[datetime, dict], None, None]:
        for timestamp in sorted(self._data.keys()):
            yield timestamp, self._data[timestamp]


    def _load_fit_file(self) -> bool:
        def mesg_listener(mesg_num: int, message: dict) -> None:
            if mesg_num == Profile['mesg_num']['RECORD']: # type: ignore
                self._handle_record_message(message)
            elif mesg_num == Profile['mesg_num']['EVENT']: # type: ignore
                self._handle_event_message(message)
            elif mesg_num == Profile['mesg_num']['CLIMB_PRO']: # type: ignore
                self._handle_climb_message(message)

        try:
            stream = Stream.from_file(self.fit_file)
            decoder = Decoder(stream)
            _, errors = decoder.read(mesg_listener=mesg_listener)

            if errors:
                logging.error(f"Errors decoding fit file:")
                for error in errors:
                    logging.error(f" - {error}")
                return False
        except Exception as e:
            logging.error(f"Failed to read fit file: {e}")
            return False
        return True


    def _handle_record_message(self, message: dict) -> None:
        if 'timestamp' not in message:
            logging.warning("RECORD message without timestamp field.")
            return
        
        timestamp = message['timestamp']
        if timestamp not in self._data:
            self._data[timestamp] = {}

        record_data = {}

        record_data['timestamp'] = timestamp
        
        if 'position_lat' in message:
            record_data['position_lat'] = message['position_lat'] * SEMICIRCLES_FACTOR
        if 'position_long' in message:
            record_data['position_long'] = message['position_long'] * SEMICIRCLES_FACTOR

        if 'enhanced_altitude' in message:
            record_data['altitude'] = message['enhanced_altitude']
        elif 'altitude' in message:
            record_data['altitude'] = message['altitude']

        if 'heart_rate' in message:
            record_data['heart_rate'] = message['heart_rate']
        if 'cadence' in message:
            record_data['cadence'] = message['cadence']
        if 'distance' in message:
            record_data['distance'] = message['distance']

        if 'enhanced_speed' in message:
            record_data['speed'] = message['enhanced_speed']
        elif 'speed' in message:
            record_data['speed'] = message['speed']

        if 'power' in message:
            record_data['power'] = message['power']
        if 'grade' in message:
            record_data['grade'] = message['grade']
        if 'temperature' in message:
            record_data['temperature'] = message['temperature']
        if 'accumulated_power' in message:
            record_data['accumulated_power'] = message['accumulated_power']
        if 'left_right_balance' in message:
            record_data['left_right_balance'] = message['left_right_balance']
        if 'gps_accuracy' in message:
            record_data['gps_accuracy'] = message['gps_accuracy']
        if 'vertical_speed' in message:
            record_data['vertical_speed'] = message['vertical_speed']
        if 'calories' in message:
            record_data['calories'] = message['calories']
        if 'left_torque_effectiveness' in message:
            record_data['left_torque_effectiveness'] = message['left_torque_effectiveness']
        if 'right_torque_effectiveness' in message:
            record_data['right_torque_effectiveness'] = message['right_torque_effectiveness']
        if 'left_pedal_smoothness' in message:
            record_data['left_pedal_smoothness'] = message['left_pedal_smoothness']
        if 'right_pedal_smoothness' in message:
            record_data['right_pedal_smoothness'] = message['right_pedal_smoothness']
        if 'combined_pedal_smoothness' in message:
            record_data['combined_pedal_smoothness'] = message['combined_pedal_smoothness']
        if 'enhanced_respiration_rate' in message:
            record_data['respiration_rate'] = message['enhanced_respiration_rate']
        if 'grit' in message:
            record_data['grit'] = message['grit']
        if 'flow' in message:
            record_data['flow'] = message['flow']
        if 'core_temperature' in message:
            record_data['core_temperature'] = message['core_temperature']

        self._data[timestamp].update(record_data)

        for key,value in self._cache.items():
            if key not in self._data[timestamp]:
                self._data[timestamp][key] = value


    def _handle_event_message(self, message: dict) -> None:
        if 'timestamp' not in message:
            logging.warning("EVENT message without timestamp field.")
            return
        if 'event' not in message:
            logging.warning("EVENT message without event field.")
            return
        if 'event_type' not in message:
            logging.warning("EVENT message without event_type field.")
            return

        timestamp = message['timestamp']
        if timestamp not in self._data:
            self._data[timestamp] = {}

        data = {}
        if message['event'] == 'front_gear_change' and message['event_type'] == 'marker':
            front_gear_num = message.get('front_gear_num', None)
            if isinstance(front_gear_num, int) and 0 < front_gear_num < 255:
                data['front_gear_num'] = front_gear_num

            front_gear = message.get('front_gear', None)
            if isinstance(front_gear, int) and 0 < front_gear < 255:
                data['front_gear'] = front_gear

        if message['event'] == 'rear_gear_change' and message['event_type'] == 'marker':
            rear_gear_num = message.get('rear_gear_num', None)
            if isinstance(rear_gear_num, int) and 0 < rear_gear_num < 255:
                data['rear_gear_num'] = rear_gear_num

            rear_gear = message.get('rear_gear', None)
            if isinstance(rear_gear, int) and 0 < rear_gear < 255:
                data['rear_gear'] = rear_gear

        self._data[timestamp].update(data)
        self._cache.update(data)


    def _handle_climb_message(self, message: dict) -> None:
        if 'timestamp' not in message:
            logging.warning("CLIMB_PRO message without timestamp field.")
            return
        if 'climb_pro_event' not in message:
            logging.warning("CLIMB_PRO message without climb_pro_event field.")
            return
        if 'climb_number' not in message:
            logging.warning("CLIMB_PRO message without climb_number field.")
            return

        timestamp = message['timestamp']
        if timestamp not in self._data:
            self._data[timestamp] = {}

        if message['climb_pro_event'] == 'start':
            climb = message['climb_number']

            self._data[timestamp]['active_climb'] = climb
            self._cache['active_climb'] = climb
        elif message['climb_pro_event'] == 'complete':
            if 'active_climb' not in self._cache:
                logging.info('Received climb_pro complete event without climb_pro start event. Updating climb active from start.')
                climb = message['climb_number']
                for t,r in self.data:
                    if t < timestamp:
                        r['active_climb'] = climb

            if 'active_climb' in self._data[timestamp]:
                del self._data[timestamp]['active_climb']
            if 'active_climb' in self._cache:
                del self._cache['active_climb']


    def _generate_calculated_fields(self) -> None:
        self._calculate_activity_time()
        self._calculate_distance()
        self._calculate_smooth_altitude()
        self._calculate_speed()
        self._calculate_power_rolling_averages()
        self._calculate_grade()
        self._calculate_vertical_speed()


    def _calculate_activity_time(self) -> None:
        logging.debug("Calculating activity time")

        start_time = None
        for timestamp,record in self.data:
            if start_time is None:
                start_time = timestamp

            record['time'] = (timestamp - start_time).total_seconds()


    def _calculate_distance(self) -> None:
        logging.debug("Calculating distance")

        last_lat = None
        last_lon = None
        total_distance = 0.0

        for timestamp,record in self.data:
            if 'position_lat' in record and 'position_long' in record:
                lat = record['position_lat']
                lon = record['position_long']

                if last_lat is not None and last_lon is not None:
                    total_distance += geo_distance(last_lat, last_lon, lat, lon)

                last_lat = lat
                last_lon = lon

            record['track_distance'] = total_distance
            if 'distance' not in record:
                record['distance'] = total_distance


    def _calculate_smooth_altitude(self) -> None:
        logging.debug("Calculating smooth altitude")

        for record, window in self._sliding_window(SMOOTH_ALTITUDE_TIME_WINDOW, 'time'):
            altitudes = [r['altitude'] for r in window if 'altitude' in r]
            if len(altitudes) > 0:
                record['smooth_altitude'] = statistics.mean(altitudes)


    def _calculate_speed(self) -> None:
        logging.debug("Calculating speed")

        last_time = None
        last_distance = None

        for timestamp,record in self.data:
            if 'distance' in record and 'time' in record:
                distance = record['distance']
                time = record['time']

                if last_time is not None and last_distance is not None:
                    time_delta = time - last_time
                    if time_delta > 0:
                        speed = (distance - last_distance) / time_delta
                        record['track_speed'] = speed
                        if 'speed' not in record:
                            record['speed'] = speed

                last_time = time
                last_distance = distance


    def _calculate_power_rolling_averages(self) -> None:
        logging.debug("Calculating power rolling averages (3s, 10s, 30s)")

        power = {}
        for timestamp,record in self.data:
            if 'power' in  record:
                power[timestamp] = record['power']

            power3s = [p[1] for p in power.items() if p[0] > timestamp - timedelta(seconds=3)]
            power10s = [p[1] for p in power.items() if p[0] > timestamp - timedelta(seconds=10)]
            power30s = [p[1] for p in power.items() if p[0] > timestamp - timedelta(seconds=30)]
            if len(power3s) > 0:
                record['power3s'] = statistics.mean(power3s)
            if len(power10s) > 0:
                record['power10s'] = statistics.mean(power10s)
            if len(power30s) > 0:
                record['power30s'] = statistics.mean(power30s)

            power = {k: v for k, v in power.items() if k > timestamp - timedelta(seconds=30)}


    def _calculate_grade(self) -> None:
        logging.debug("Calculating grade")

        alt_key = 'smooth_altitude'
        dist_key = 'distance'

        for record, window in self._sliding_window(MAX_GRADE_WINDOW, dist_key):
            dist = record.get(dist_key, None)
            alt = record.get(alt_key, None)
            if dist is None or alt is None:
                continue

            altitudes = [(r[dist_key], r[alt_key]) for r in window if alt_key in r and dist_key in r]
            z1,y1 = altitudes[0]
            z2,y2 = altitudes[-1]

            if dist - z1 < MIN_GRADE_WINDOW/2:
                continue # don't calculate grade - covers beginning of activity
            if z2 - dist < MIN_GRADE_WINDOW/2:
                continue # don't calculate grade - covers end of activity

            z = z2 - z1
            y = y2 - y1

            x = math.sqrt(z**2 - y**2) # pythagoras (x**2 + y**2 = z**2 where z is distance delta and y is altitude delta)

            record['grade'] = (y / x) * 100.0


    def _calculate_vertical_speed(self) -> None:
        logging.debug("Calculating vertical speed")

        last_time = None
        last_altitude = None

        for timestamp,record in self.data:
            if 'altitude' in record and 'time' in record:
                altitude = record['altitude']
                time = record['time']

                if 'vertical_speed' not in record and last_time is not None and last_altitude is not None:
                    time_delta = time - last_time
                    if time_delta > 0:
                        vertical_speed = (altitude - last_altitude) / time_delta
                        record['vertical_speed'] = vertical_speed

                last_time = time
                last_altitude = altitude


    def _sliding_window(self, window_size: float, key: str) -> Generator[tuple[dict, list[dict]], None, None]:
        def in_window(record: dict, target: float) -> bool:
            value = record.get(key, None)
            if value is None:
                return False
            delta: float = abs(value - target)
            return delta <= (window_size / 2.0)

        seq = [record for _,record in self.data]

        for i, cur in enumerate(seq):
            value = cur.get(key, None)
            if value is None:
                logging.warning(f"Record without {key} field in sliding window calculation. Skipping.")
                continue

            # backward until condition fails
            left = []
            j = i - 1
            while j >= 0 and in_window(seq[j], value):
                left.append(seq[j])
                j -= 1
            left.reverse()

            # forward until condition fails
            right = []
            k = i + 1
            while k < len(seq) and in_window(seq[k], value):
                right.append(seq[k])
                k += 1

            yield cur, left + [cur] + right


# COVERED:

# "record" - 5414 messages:
# {'cadence', 137, 138, 144, 'enhanced_speed', 'enhanced_respiration_rate', 'position_long', 'timestamp', 'left_pedal_smoothness', 'left_torque_effectiveness', 'distance', 'right_torque_effectiveness', 'heart_rate', 'fractional_cadence', 'flow', 'power', 'accumulated_power', 'right_pedal_smoothness', 'enhanced_altitude', 90, 'position_lat', 'temperature', 107, 'grit'}

# "event" - 270 messages:
# {'rear_gear', 'gear_change_data', 'rear_gear_num', 'event_group', 'event_type', 'timer_trigger', 'data', 'front_gear_num', 'front_gear', 'event', 'timestamp'}


# "climb_pro" - 3 messages:
# {'position_lat', 'climb_pro_event', 'climb_number', 'current_dist', 'climb_category', 'position_long', 'timestamp'}


# ---


# FOR RECORDS:


# ---


# FOR SUMMARY (maybe):

# "file_id" - 1 messages:
# {'type', 'garmin_product', 'time_created', 'product', 'manufacturer', 'serial_number'}

# "activity" - 1 messages:
# {'type', 'num_sessions', 'total_timer_time', 'event_type', 'local_timestamp', 'event', 'timestamp'}

# "session" - 1 messages:
# {'normalized_power', 'threshold_power', 'avg_cadence', 'swc_long', 'total_elapsed_time', 'swc_lat', 138, 'avg_right_torque_effectiveness', 'training_load_peak', 'event', 'end_position_long', 'sport', 'total_ascent', 'enhanced_avg_speed', 'enhanced_max_respiration_rate', 'sport_profile_name', 'avg_temperature', 'timestamp', 'total_calories', 'avg_vam', 'trigger', 'total_anaerobic_training_effect', 'max_temperature', 'avg_right_pedal_smoothness', 'max_power', 'avg_heart_rate', 178, 'total_training_effect', 'start_time', 'total_grit', 'jump_count', 'sub_sport', 'intensity_factor', 'avg_fractional_cadence', 'num_laps', 'nec_lat', 184, 'avg_flow', 188, 'total_strokes', 'max_cadence', 'enhanced_max_speed', 'total_timer_time', 'total_cycles', 196, 'event_type', 205, 'end_position_lat', 206, 207, 81, 'enhanced_avg_respiration_rate', 'total_distance', 'first_lap_index', 219, 'total_work', 'start_position_lat', 'enhanced_min_respiration_rate', 'training_stress_score', 'min_temperature', 106, 'start_position_long', 108, 'max_fractional_cadence', 'message_index', 'avg_power', 'max_heart_rate', 'total_descent', 'nec_long'}

# "device_settings" - 1 messages:
# {'time_zone_offset', 3, 10, 11, 13, 14, 15, 144, 22, 26, 'activity_tracker_enabled', 29, 159, 33, 35, 38, 'active_time_zone', 41, 170, 173, 'lactate_threshold_autodetect_enabled', 48, 52, 53, 54, 'autosync_min_steps', 63, 'date_mode', 75, 77, 81, 85, 218, 91, 219, 'utc_offset', 97, 98, 'backlight_mode', 106, 109, 110, 111, 'time_mode', 121, 'time_offset', 'autosync_min_time'}

# "user_profile" - 1 messages:
# {'dist_setting', 'friendly_name', 'wake_time', 'height', 'weight', 'gender', 'hr_setting', 'temperature_setting', 'height_setting', 24, 'default_max_heart_rate', 36, 40, 'weight_setting', 42, 'age', 'default_max_biking_heart_rate', 'power_setting', 44, 45, 'position_setting', 'resting_heart_rate', 57, 60, 62, 'activity_class', 65, 66, 67, 'speed_setting', 69, 'sleep_time', 'language', 'elev_setting'}

# "sport" - 1 messages:
# {5, 6, 9, 10, 12, 15, 17, 'sport', 18, 'sub_sport', 22, 23, 24, 'name'}

# "zones_target" - 1 messages:
# {'functional_threshold_power', 9, 10, 11, 12, 13, 'hr_calc_type', 'max_heart_rate', 'threshold_heart_rate', 'pwr_calc_type', 254}

# "training_file" - 1 messages:
# {'type', 'garmin_product', 'time_created', 'product', 'manufacturer', 'serial_number', 'timestamp'}


# ---


# TBD:

# "segment_lap" - 1 messages:
# {'max_speed', 'normalized_power', 'avg_cadence', 'swc_long', 'total_elapsed_time', 'swc_lat', 'avg_right_torque_effectiveness', 'end_position_long', 'sport', 'total_ascent', 'status', 'timestamp', 'total_calories', 'avg_right_pedal_smoothness', 'avg_speed', 'max_power', 'avg_heart_rate', 'avg_fractional_cadence', 'start_time', 'total_grit', 'nec_lat', 'total_strokes', 'avg_flow', 'max_cadence', 'total_timer_time', 'total_cycles', 'uuid', 'end_position_lat', 'manufacturer', 88, 'total_distance', 'total_work', 'start_position_lat', 'max_fractional_cadence', 'start_position_long', 'message_index', 'avg_power', 'max_heart_rate', 'total_descent', 'nec_long', 'name'}

# "hrv" - 5377 messages:
# {'time'}

# "time_in_zone" - 74 messages:
# {'reference_mesg', 'functional_threshold_power', 'hr_zone_high_boundary', 'reference_index', 'resting_heart_rate', 18, 'hr_calc_type', 'time_in_power_zone', 'time_in_hr_zone', 'max_heart_rate', 'threshold_heart_rate', 'pwr_calc_type', 'power_zone_high_boundary', 'timestamp'}

# "lap" - 1 messages:
# {'normalized_power', 'avg_cadence', 'total_elapsed_time', 'avg_right_torque_effectiveness', 'event', 'end_position_long', 145, 'sport', 'total_ascent', 'enhanced_avg_speed', 'enhanced_max_respiration_rate', 27, 28, 29, 30, 'timestamp', 'total_calories', 'avg_vam', 155, 'avg_temperature', 'max_temperature', 163, 'avg_right_pedal_smoothness', 'max_power', 'avg_heart_rate', 'avg_fractional_cadence', 'start_time', 'total_grit', 'jump_count', 'sub_sport', 'total_strokes', 'avg_flow', 'max_cadence', 'enhanced_max_speed', 'total_timer_time', 'total_cycles', 'event_type', 'end_position_lat', 'enhanced_avg_respiration_rate', 'total_distance', 'total_work', 'start_position_lat', 97, 'min_temperature', 'max_fractional_cadence', 'start_position_long', 'message_index', 'avg_power', 'max_heart_rate', 'lap_trigger', 'total_descent'}

# "split" - 72 messages:
# {'max_speed', 132, 133, 7, 'total_elapsed_time', 135, 136, 11, 12, 15, 16, 17, 18, 19, 20, 'end_position_long', 'total_ascent', 29, 30, 'total_calories', 32, 33, 34, 40, 41, 'start_elevation', 'avg_speed', 42, 'start_time', 54, 56, 'end_time', 58, 67, 'total_timer_time', 'end_position_lat', 'avg_vert_speed', 79, 88, 89, 'total_distance', 90, 92, 91, 'start_position_lat', 99, 101, 102, 103, 104, 105, 106, 'start_position_long', 107, 108, 109, 'message_index', 117, 118, 'total_descent', 124, 253, 'split_type'}

# "split_summary" - 3 messages:
# {'max_speed', 14, 15, 17, 19, 'total_ascent', 25, 26, 27, 'total_calories', 39, 41, 'avg_speed', 43, 'avg_heart_rate', 52, 53, 60, 64, 'num_splits', 'total_timer_time', 'avg_vert_speed', 79, 85, 86, 88, 'total_distance', 'message_index', 'max_heart_rate', 253, 'total_descent', 'split_type'}

# "timestamp_correlation" - 1 messages:
# {'system_timestamp', 'local_timestamp', 'timestamp'}

# "device_info" - 14 messages:
# {'ant_network', 'cum_operating_time', 'ant_device_type', 9, 13, 'battery_status', 15, 16, 'device_type', 'battery_voltage', 24, 'serial_number', 'device_index', 29, 30, 'timestamp', 'local_device_type', 31, 'software_version', 'product', 'antplus_device_type', 'hardware_version', 'garmin_product', 'manufacturer', 'source_type', 'battery_level'}

# "device_aux_battery_info" - 4 messages:
# {'battery_identifier', 'device_index', 'battery_status', 'timestamp'}
