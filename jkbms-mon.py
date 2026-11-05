#!/usr/bin/env python3

"""
JK PB-series BMS RS485 -> MQTT

Поддерживает два режима:

1. PARALLEL_PASSIVE = True
   Пассивно принимает пакеты из parallel-порта. Ничего в RS485 не передаёт.

2. PARALLEL_PASSIVE = False
   Периодически отправляет Modbus RTU-команду запроса runtime-данных:
       <slave> 10 16 20 00 01 02 00 00 CRC16

Из потока выделяются кадры:
    55 AA EB 90 02 00 ...

Тип 0x02 рассматривается как runtime-блок 0x1200.
Данные публикуются в MQTT как JSON.
"""

from __future__ import annotations

import json
import logging
import struct
import sys
import time
from typing import Any

import serial
import paho.mqtt.publish as publish


# ---------------------------------------------------------------------------
# Настройки Serial
# ---------------------------------------------------------------------------

SERIAL_PORT = "/dev/ttyUSB1"
SERIAL_BAUDRATE = 115200
SERIAL_BYTESIZE = 8
SERIAL_PARITY = "N"
SERIAL_STOPBITS = 1
SERIAL_TIMEOUT = 0.10

# В дампах полный пакет 55 AA EB 90 ... занимал 308 байт.
JK_HEADER = bytes.fromhex("55 AA EB 90")
JK_PACKET_SIZE = 308
JK_RUNTIME_PACKET_TYPE = 0x02

# True: ничего не передавать в BMS, только слушать parallel-порт.
# False: посылать запрос runtime-пакета через регистр 0x1620.
PARALLEL_PASSIVE = True

# Используется только при PARALLEL_PASSIVE = False.
MODBUS_SLAVE_ID = 1
REQUEST_INTERVAL_SECONDS = 2.0
RESPONSE_TIMEOUT_SECONDS = 1.5


# ---------------------------------------------------------------------------
# Настройки MQTT
# ---------------------------------------------------------------------------

MQTT_HOST = "192.168.1.11"
MQTT_PORT = 1883
MQTT_TOPIC_JKBMS = "jkbms/battery"
MQTT_TOPIC_JKBMS_SETTINGS = "jkbms/settings"
MQTT_USERNAME = "mqtt_user"
MQTT_PASSWORD = "ChangeMe"
MQTT_QOS = 0
MQTT_RETAIN = False

# Ограничение частоты публикации. 0 = публиковать каждый runtime-пакет.
MQTT_MIN_PUBLISH_INTERVAL_SECONDS = 0.0

# Количество ячеек в батареях
CELLS_COUNT = 16

mqtt_auth = None
if MQTT_USERNAME:
    mqtt_auth = {
        "username": MQTT_USERNAME,
        "password": MQTT_PASSWORD or "",
    }


# ---------------------------------------------------------------------------
# Логирование
# ---------------------------------------------------------------------------

LOG_LEVEL = logging.INFO
DUMP_RAW_PACKETS = False

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("jkbms")


# ---------------------------------------------------------------------------
# Вспомогательное чтение little-endian
# ---------------------------------------------------------------------------

def u8(data: bytes, offset: int) -> int:
    return data[offset]


def u16(data: bytes, offset: int) -> int:
    return struct.unpack_from("<H", data, offset)[0]


def i16(data: bytes, offset: int) -> int:
    return struct.unpack_from("<h", data, offset)[0]


def u32(data: bytes, offset: int) -> int:
    return struct.unpack_from("<I", data, offset)[0]


def i32(data: bytes, offset: int) -> int:
    return struct.unpack_from("<i", data, offset)[0]


def f32(data: bytes, offset: int) -> float:
    return struct.unpack_from("<f", data, offset)[0]


# ---------------------------------------------------------------------------
# Modbus RTU: формирование запроса на 0x1620
# ---------------------------------------------------------------------------

def modbus_crc16(data: bytes) -> int:
    """Modbus CRC16. Возвращает число, в кадр записывается little-endian."""
    crc = 0xFFFF

    for byte in data:
        crc ^= byte

        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1

    return crc & 0xFFFF


def build_write_register_request(
    slave_id: int,
    register: int,
    value: int = 0,
) -> bytes:
    """
    Function 0x10, запись одного 16-битного регистра.

    Пример для slave=1, register=0x1620, value=0:
        01 10 16 20 00 01 02 00 00 D6 F1
    """
    if not 1 <= slave_id <= 247:
        raise ValueError("Modbus slave ID должен быть в диапазоне 1..247")

    body = bytes(
        [
            slave_id,
            0x10,
            (register >> 8) & 0xFF,
            register & 0xFF,
            0x00,
            0x01,
            0x02,
            (value >> 8) & 0xFF,
            value & 0xFF,
        ]
    )

    crc = modbus_crc16(body)
    return body + struct.pack("<H", crc)


RUNTIME_REQUEST = build_write_register_request(
    MODBUS_SLAVE_ID,
    register=0x1620,
    value=0,
)


# ---------------------------------------------------------------------------
# Разбор runtime-блока 0x1200
# ---------------------------------------------------------------------------

ALARM_BITS = {
    0: "alarm_wire_resistance",
    1: "alarm_mos_over_temperature",
    2: "alarm_cell_quantity",
    3: "alarm_current_sensor",
    4: "alarm_cell_over_voltage",
    5: "alarm_battery_over_voltage",
    6: "alarm_charge_over_current",
    7: "alarm_charge_short_circuit",
    8: "alarm_charge_over_temperature",
    9: "alarm_charge_under_temperature",
    10: "alarm_internal_communication",
    11: "alarm_cell_under_voltage",
    12: "alarm_battery_under_voltage",
    13: "alarm_discharge_over_current",
    14: "alarm_discharge_short_circuit",
    15: "alarm_discharge_over_temperature",
    16: "alarm_charge_mos",
    17: "alarm_discharge_mos",
    18: "alarm_gps_disconnected",
    19: "alarm_modify_password",
    20: "alarm_discharge_on_failed",
    21: "alarm_battery_over_temperature",
    22: "alarm_temperature_sensor",
    23: "alarm_parallel_module",
}


def decode_alarm_bits(value: int) -> dict[str, bool]:
    return {
        name: bool(value & (1 << bit))
        for bit, name in ALARM_BITS.items()
    }

def get_bms_address(packet: bytes) -> int:
    if len(packet) != JK_PACKET_SIZE:
        raise ValueError("Неверная длина пакета")

    trailer = packet[-8:]

    address = trailer[0]
    function = trailer[1]
    register = (trailer[2] << 8) | trailer[3]
    register_count = (trailer[4] << 8) | trailer[5]

    if function != 0x10:
        raise ValueError(
            f"Неверная функция в конце пакета: 0x{function:02X}"
        )

    if register not in (0x161E, 0x1620):
        raise ValueError(
            f"Неожиданный регистр: 0x{register:04X}"
        )

    if register_count != 1:
        raise ValueError(
            f"Неожиданное количество регистров: {register_count}"
        )

    expected_crc = modbus_crc16(trailer[:6])
    received_crc = struct.unpack_from("<H", trailer, 6)[0]

    if received_crc != expected_crc:
        raise ValueError(
            f"Неверный CRC Modbus trailer: "
            f"получен 0x{received_crc:04X}, "
            f"ожидался 0x{expected_crc:04X}"
        )

    return address


def parse_settings_packet(packet: bytes) -> dict[str, Any]:
    """Разобрать пакет типа 0x01 — блок настроек 0x1000."""
    if len(packet) != JK_PACKET_SIZE:
        raise ValueError(
            f"Неверная длина JK-пакета: {len(packet)}, "
            f"ожидалось {JK_PACKET_SIZE}"
        )

    if packet[:4] != JK_HEADER:
        raise ValueError("Неверный заголовок JK-пакета")

    packet_type = packet[4]
    if packet_type != 0x01:
        raise ValueError(
            f"Ожидался settings-пакет 0x01, получен 0x{packet_type:02X}"
        )

    data = packet[6:]
    bms_address = get_bms_address(packet)

    result: dict[str, Any] = {
        "timestamp": int(time.time()),
        "bms_address": bms_address,
        "packet_type": packet_type,
        "packet_subtype": packet[5],
    }

    # Напряжения и пороги, исходно в mV.
    voltage_fields = {
        0x0000: "smart_sleep_voltage",
        0x0004: "cell_undervoltage_protection",
        0x0008: "cell_undervoltage_recovery",
        0x000C: "cell_overvoltage_protection",
        0x0010: "cell_overvoltage_recovery",
        0x0014: "balance_trigger_delta",
        0x0018: "soc_100_voltage",
        0x001C: "soc_0_voltage",
        0x0020: "recommended_charge_voltage",
        0x0024: "float_voltage",
        0x0028: "power_off_voltage",
    }
    for offset, name in voltage_fields.items():
        result[name] = round(u32(data, offset) / 1000.0, 3)

    result["max_charge_current"] = round(u32(data, 0x002C) / 1000.0, 3)
    result["charge_overcurrent_delay"] = u32(data, 0x0030)
    result["charge_overcurrent_recovery_delay"] = u32(data, 0x0034)
    result["max_discharge_current"] = round(u32(data, 0x0038) / 1000.0, 3)
    result["discharge_overcurrent_delay"] = u32(data, 0x003C)
    result["discharge_overcurrent_recovery_delay"] = u32(data, 0x0040)
    result["short_circuit_recovery_delay"] = u32(data, 0x0044)
    result["max_balance_current"] = round(u32(data, 0x0048) / 1000.0, 3)

    temperature_fields = {
        0x004C: "charge_overtemperature_protection",
        0x0050: "charge_overtemperature_recovery",
        0x0054: "discharge_overtemperature_protection",
        0x0058: "discharge_overtemperature_recovery",
        0x005C: "charge_undertemperature_protection",
        0x0060: "charge_undertemperature_recovery",
        0x0064: "mos_overtemperature_protection",
        0x0068: "mos_overtemperature_recovery",
    }
    for offset, name in temperature_fields.items():
        result[name] = round(i32(data, offset) / 10.0, 1)

    result["cell_count"] = u32(data, 0x006C)
    result["charge_enabled"] = bool(u32(data, 0x0070))
    result["discharge_enabled"] = bool(u32(data, 0x0074))
    result["balance_enabled"] = bool(u32(data, 0x0078))
    result["design_capacity"] = round(u32(data, 0x007C) / 1000.0, 3)
    result["short_circuit_delay_us"] = u32(data, 0x0080)
    result["balance_start_voltage"] = round(u32(data, 0x0084) / 1000.0, 3)

    # В блоке настроек всегда передаём все 32 значения.
    for index in range(32):
        result[f"cell_{index + 1:02d}_wire_resistance_uohm"] = u32(
            data,
            0x0088 + index * 4,
        )

    result["device_address"] = u32(data, 0x0108)
    result["precharge_time"] = u32(data, 0x010C)

    feature_flags = u32(data, 0x0110)
    result["feature_flags"] = feature_flags
    result["heater_enabled"] = bool(feature_flags & (1 << 0))
    result["temperature_sensors_disabled"] = bool(feature_flags & (1 << 1))
    result["gps_heartbeat_enabled"] = bool(feature_flags & (1 << 2))
    result["port_is_rs485"] = bool(feature_flags & (1 << 3))
    result["lcd_always_on"] = bool(feature_flags & (1 << 4))
    result["special_charger_enabled"] = bool(feature_flags & (1 << 5))
    result["smart_sleep_enabled"] = bool(feature_flags & (1 << 6))
    result["parallel_current_limit_disabled"] = bool(feature_flags & (1 << 7))
    result["timed_data_storage_enabled"] = bool(feature_flags & (1 << 8))
    result["charge_float_mode_enabled"] = bool(feature_flags & (1 << 9))

    result["smart_sleep_time"] = u16(data, 0x0114)
    result["data_field_enable"] = u8(data, 0x0118)

    return result


def parse_runtime_packet(packet: bytes) -> dict[str, Any]:
    if len(packet) != JK_PACKET_SIZE:
        raise ValueError(
            f"Неверная длина JK-пакета: {len(packet)}, "
            f"ожидалось {JK_PACKET_SIZE}"
        )

    if packet[:4] != JK_HEADER:
        raise ValueError("Неверный заголовок JK-пакета")

    packet_type = packet[4]
    if packet_type != JK_RUNTIME_PACKET_TYPE:
        raise ValueError(
            f"Ожидался runtime-пакет 0x02, получен 0x{packet_type:02X}"
        )

    # packet[5] пока не идентифицирован.
    # Полезная область 0x1200 начинается с packet[6].
    data = packet[6:]

    bms_address = get_bms_address(packet)

    # В документе runtime-блок описан минимум до offset 0x010D.
    if len(data) < 0x010E:
        raise ValueError("Runtime-блок короче ожидаемого")

    result: dict[str, Any] = {
        "timestamp": int(time.time()),
        "bms_address": bms_address,
        "packet_type": packet_type,
        "packet_subtype": packet[5],
    }

    # 0x0000..0x003E: 32 напряжения ячеек, UINT16, mV.
    cells_mv = [u16(data, index * 2) for index in range(CELLS_COUNT)]

    active_cell_count = 0
    active_cells_mv: list[int] = []

    for index, value_mv in enumerate(cells_mv, start=1):
        # Неиспользуемые каналы встречаются как 0 или 0xFFFF.
        is_active = value_mv not in (0, 0xFFFF)
        result[f"cell_{index:02d}_voltage"] = (
            round(value_mv / 1000.0, 3) if is_active else None
        )

        if is_active:
            active_cell_count += 1
            active_cells_mv.append(value_mv)

    result["active_cell_count"] = active_cell_count

    # 0x0040..0x0049
    result["cell_status"] = u32(data, 0x0040)
    result["cell_average_voltage"] = round(u16(data, 0x0044) / 1000.0, 3)
    result["cell_delta_voltage"] = round(u16(data, 0x0046) / 1000.0, 3)
    result["max_voltage_cell_number"] = u8(data, 0x0048)
    result["min_voltage_cell_number"] = u8(data, 0x0049)

    if active_cells_mv:
        result["calculated_cell_min_voltage"] = round(
            min(active_cells_mv) / 1000.0, 3
        )
        result["calculated_cell_max_voltage"] = round(
            max(active_cells_mv) / 1000.0, 3
        )
        result["calculated_cell_delta_voltage"] = round(
            (max(active_cells_mv) - min(active_cells_mv)) / 1000.0, 3
        )

    # 0x004A..0x0088: сопротивления проводов балансировки, UINT16, mOhm.
    for index in range(CELLS_COUNT):
        result[f"cell_{index + 1:02d}_wire_resistance_mohm"] = u16(
            data,
            0x004A + index * 2,
        )

    # Основные параметры.
    result["mos_temperature"] = round(i16(data, 0x008A) / 10.0, 1)
    result["cell_wire_resistance_status"] = u32(data, 0x008C)

    result["battery_voltage"] = round(u32(data, 0x0090) / 1000.0, 3)
    result["battery_power"] = round(u32(data, 0x0094) / 1000.0, 3)
    result["battery_current"] = round(i32(data, 0x0098) / 1000.0, 3)

    result["battery_temperature_1"] = round(i16(data, 0x009C) / 10.0, 1)
    result["battery_temperature_2"] = round(i16(data, 0x009E) / 10.0, 1)

    alarms = u32(data, 0x00A0)
    result["alarms"] = alarms
    result.update(decode_alarm_bits(alarms))

    result["balance_current"] = round(i16(data, 0x00A4) / 1000.0, 3)
    result["balance_state"] = u8(data, 0x00A6)
    result["soc"] = u8(data, 0x00A7)

    result["remaining_capacity"] = round(i32(data, 0x00A8) / 1000.0, 3)
    result["full_charge_capacity"] = round(u32(data, 0x00AC) / 1000.0, 3)
    result["cycle_count"] = u32(data, 0x00B0)
    result["cycle_capacity"] = round(u32(data, 0x00B4) / 1000.0, 3)

    result["soh"] = u8(data, 0x00B8)
    result["precharge"] = bool(u8(data, 0x00B9))
    result["user_alarm"] = u16(data, 0x00BA)
    result["runtime_seconds"] = u32(data, 0x00BC)

    result["charge_enabled"] = bool(u8(data, 0x00C0))
    result["discharge_enabled"] = bool(u8(data, 0x00C1))
    result["user_alarm_2"] = u16(data, 0x00C2)

    result["discharge_overcurrent_release_time"] = u16(data, 0x00C4)
    result["discharge_short_circuit_release_time"] = u16(data, 0x00C6)
    result["charge_overcurrent_release_time"] = u16(data, 0x00C8)
    result["charge_short_circuit_release_time"] = u16(data, 0x00CA)
    result["cell_undervoltage_release_time"] = u16(data, 0x00CC)
    result["cell_overvoltage_release_time"] = u16(data, 0x00CE)

    sensor_flags = u8(data, 0x00D0)
    result["temperature_sensor_flags"] = sensor_flags
    result["heating"] = bool(u8(data, 0x00D1))

    result["emergency_time"] = u16(data, 0x00D4)
    result["discharge_current_correction"] = u16(data, 0x00D6)
    result["charge_current_sensor_voltage"] = u16(data, 0x00D8)
    result["discharge_current_sensor_voltage"] = u16(data, 0x00DA)
    result["battery_voltage_correction"] = round(f32(data, 0x00DC), 6)

    # Дополнительное напряжение из документа, единица 0.01 V.
    result["battery_voltage_secondary"] = round(
        u16(data, 0x00E4) / 100.0,
        2,
    )
    result["heating_current"] = round(i16(data, 0x00E6) / 1000.0, 3)
    result["charger_plugged"] = bool(u8(data, 0x00EF))
    result["system_run_ticks"] = u32(data, 0x00F0)

    result["battery_temperature_3"] = round(i16(data, 0x00F8) / 10.0, 1)
    result["battery_temperature_4"] = round(i16(data, 0x00FA) / 10.0, 1)
    result["battery_temperature_5"] = round(i16(data, 0x00FC) / 10.0, 1)

    result["rtc_ticks"] = u32(data, 0x0100)
    result["time_enter_sleep"] = u32(data, 0x0108)
    result["parallel_current_limit_module"] = bool(u8(data, 0x010C))

    return result


# ---------------------------------------------------------------------------
# Выделение пакетов из непрерывного serial-потока
# ---------------------------------------------------------------------------

def extract_jk_packets(buffer: bytearray) -> list[bytes]:
    packets: list[bytes] = []

    while True:
        start = buffer.find(JK_HEADER)

        if start < 0:
            # Сохраняем хвост на случай разорванного заголовка.
            if len(buffer) > len(JK_HEADER) - 1:
                del buffer[: -(len(JK_HEADER) - 1)]
            break

        if start > 0:
            del buffer[:start]

        if len(buffer) < JK_PACKET_SIZE:
            break

        packet = bytes(buffer[:JK_PACKET_SIZE])
        del buffer[:JK_PACKET_SIZE]
        packets.append(packet)

    return packets


# ---------------------------------------------------------------------------
# MQTT
# ---------------------------------------------------------------------------

def publish_mqtt(topic_base: str, data: dict[str, Any]) -> None:
    topic = f"{topic_base}/{data['bms_address']}"

    payload = json.dumps(
        data,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )

    publish.single(
        topic,
        payload=payload,
        hostname=MQTT_HOST,
        port=MQTT_PORT,
        qos=MQTT_QOS,
        retain=MQTT_RETAIN,
        auth=mqtt_auth,
    )


# ---------------------------------------------------------------------------
# Основной цикл
# ---------------------------------------------------------------------------

def open_serial() -> serial.Serial:
    return serial.Serial(
        port=SERIAL_PORT,
        baudrate=SERIAL_BAUDRATE,
        bytesize=SERIAL_BYTESIZE,
        parity=SERIAL_PARITY,
        stopbits=SERIAL_STOPBITS,
        timeout=SERIAL_TIMEOUT,
    )


def run() -> None:
    receive_buffer = bytearray()
    last_request_time = 0.0
    last_publish_time: dict[tuple[str, int], float] = {}

    while True:
        ser: serial.Serial | None = None

        try:
            log.info(
                "Открываю %s, %d 8%s%d",
                SERIAL_PORT,
                SERIAL_BAUDRATE,
                SERIAL_PARITY,
                SERIAL_STOPBITS,
            )
            ser = open_serial()
            ser.reset_input_buffer()

            if PARALLEL_PASSIVE:
                log.info("Режим: passive parallel, запросы не отправляются")
            else:
                log.info(
                    "Режим: активный опрос slave=%d, запрос=%s",
                    MODBUS_SLAVE_ID,
                    RUNTIME_REQUEST.hex(" ").upper(),
                )

            while True:
                now = time.monotonic()

                if (
                    not PARALLEL_PASSIVE
                    and now - last_request_time >= REQUEST_INTERVAL_SECONDS
                ):
                    ser.write(RUNTIME_REQUEST)
                    ser.flush()
                    last_request_time = now
                    log.debug("TX: %s", RUNTIME_REQUEST.hex(" ").upper())

                chunk = ser.read(4096)

                if not chunk:
                    continue

                receive_buffer.extend(chunk)

                for packet in extract_jk_packets(receive_buffer):
                    if DUMP_RAW_PACKETS:
                        log.info(
                            "RX JK (%d): %s",
                            len(packet),
                            packet.hex(" ").upper(),
                        )

                    packet_type = packet[4]

                    try:
                        if packet_type == 0x01:
                            values = parse_settings_packet(packet)
                            topic_base = MQTT_TOPIC_JKBMS_SETTINGS
                        elif packet_type == JK_RUNTIME_PACKET_TYPE:
                            values = parse_runtime_packet(packet)
                            topic_base = MQTT_TOPIC_JKBMS
                        else:
                            log.debug(
                                "Пропущен JK-пакет типа 0x%02X",
                                packet_type,
                            )
                            continue
                    except (ValueError, IndexError, struct.error) as exc:
                        log.warning("Ошибка разбора JK-пакета: %s", exc)
                        continue

                    now = time.monotonic()
                    publish_key = (topic_base, values["bms_address"])
                    previous_publish = last_publish_time.get(publish_key, 0.0)
                    if (
                        MQTT_MIN_PUBLISH_INTERVAL_SECONDS > 0
                        and now - previous_publish
                        < MQTT_MIN_PUBLISH_INTERVAL_SECONDS
                    ):
                        continue

                    try:
                        publish_mqtt(topic_base, values)
                    except Exception:
                        log.exception("Ошибка публикации MQTT")
                        continue

                    last_publish_time[publish_key] = now

                    if packet_type == JK_RUNTIME_PACKET_TYPE:
                        log.debug(
                            "MQTT runtime: Battery=%d, U=%.3f V, I=%.3f A, "
                            "P=%.3f W, SOC=%d%%, cells=%d, delta=%.3f V, "
                            "alarms=0x%08X",
                            values["bms_address"],
                            values["battery_voltage"],
                            values["battery_current"],
                            values["battery_power"],
                            values["soc"],
                            values["active_cell_count"],
                            values["cell_delta_voltage"],
                            values["alarms"],
                        )
                    else:
                        log.debug(
                            "MQTT settings: Battery=%d, cells=%d, "
                            "capacity=%.3f Ah, max_charge=%.3f A, "
                            "max_discharge=%.3f A",
                            values["bms_address"],
                            values["cell_count"],
                            values["design_capacity"],
                            values["max_charge_current"],
                            values["max_discharge_current"],
                        )

        except serial.SerialException as exc:
            log.error("Ошибка serial: %s", exc)
        except KeyboardInterrupt:
            log.info("Остановлено пользователем")
            return
        except Exception:
            log.exception("Неожиданная ошибка")
        finally:
            if ser is not None and ser.is_open:
                ser.close()

        receive_buffer.clear()
        log.info("Повторное подключение через 3 секунды")
        time.sleep(3)


if __name__ == "__main__":
    print("JK BMS Monitoring, version 0.1 (beta) by DuMONChIK (dima@dshylov.com)")
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(0)
