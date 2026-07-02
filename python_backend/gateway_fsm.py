import socket
import time
import os
import json
import asyncio
import threading
import sqlite3
import math
import select
from datetime import datetime
import websockets

# ==================================================
# ⚙️ CONFIGURATION
# ==================================================
LOOP_DELAY      = 0.02     # 20 ms
BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
DB_FILE         = os.path.join(BASE_DIR, "smd_packing_analytics.db")

RUST_HOST       = "127.0.0.1"
RUST_PORT       = 8766

STATES = [
    'LOAD_CARRIER', 'INDEX_CARRIER', 'POWER_ON', 'SET_PARAMS', 
    'SENSOR_CHECK_CARRIER', 'READY', 'LOAD_PART', 'VISION', 
    'CHECK_TEMP', 'FEED_CARRIER', 'COUNT_PROCESS', 'COUNT_CHECK', 
    'COUNT_ACCUMULATE', 'SEAL_PROCESS', 'VISION_QC', 'TAKEUP_REEL', 'ALARM'
]

system_data = {
    "current_state": "READY",
    "running": False,
    "mode": "auto",           
    "step_allowed": False,     
    "cycles": 0,
    "speed_mul": 1.0,
    "ip0": 0, "ip1": 0,
    "op0": 0, "op1": 0,
    "camera1_count": 0,
    "encoder_count": 0,
    "pieces_count": 0,
    "target_pieces": 5,
    "predictive_warning": "" 
}

step_history_cache = {"FEED_CARRIER": [], "SEAL_PROCESS": []}
connected_clients = set()
trigger_reset_timer = False

# 🚀 ตัวแปร Global สำหรับถือสายท่อ Socket ค้างไว้แบบถาวร
rust_socket = None

# ==================================================
# 🔌 PERSISTENT SOCKET CONNECTION TO RUST
# ==================================================
def connect_to_rust_layer():
    global rust_socket
    if rust_socket is not None:
        return True
    try:
        rust_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        rust_socket.settimeout(0.1)
        rust_socket.connect((RUST_HOST, RUST_PORT))
        print("💡 [Python -> Rust] Persistent Connection Established!")
        return True
    except:
        rust_socket = None
        return False

def send_data_to_rust_layer(state_name, is_running, op0, ip0, cycles):
    global rust_socket
    if not connect_to_rust_layer():
        return ip0
        
    try:
        payload = json.dumps({
            "current_state": state_name,
            "running": is_running,
            "op0": op0,
            "ip0": ip0,
            "cycles": cycles
        }) + "\n"
        rust_socket.sendall(payload.encode('utf-8'))
        
        # ดักรอฟังบิตเซนเซอร์ที่ Rust ส่งสวนกลับมา
        ready = select.select([rust_socket], [], [], 0.02)
        if ready[0]:
            resp = rust_socket.recv(1024).decode('utf-8')
            if not resp:
                # ถ้าสายหลุด ให้สั่งเคลียร์ตัวแปรเพื่อรอบหน้าจะทำการ Reconnect ออโต้
                rust_socket.close()
                rust_socket = None
                return ip0
            data = json.loads(resp.strip())
            return data.get("ip0", ip0)
    except:
        try:
            rust_socket.close()
        except: pass
        rust_socket = None
    return ip0

# ==================================================
# 🗄️ SQLITE DATABASE FUNCTIONS
# ==================================================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS machine_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            state_name TEXT NOT NULL,
            duration_sec REAL NOT NULL,
            control_mode TEXT NOT NULL,
            current_cycle INTEGER NOT NULL,
            status TEXT NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS production_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            end_timestamp TEXT NOT NULL,
            cycle_number INTEGER NOT NULL,
            total_duration_sec REAL NOT NULL
        )
    """)
    conn.commit()
    conn.close()

def log_state_transition_to_sql(state_name, duration_sec, mode, cycles, status="NORMAL"):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        cursor.execute("""
            INSERT INTO machine_logs (timestamp, state_name, duration_sec, control_mode, current_cycle, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (now_str, state_name, round(duration_sec, 3), mode, cycles, status))
        conn.commit()
        conn.close()
        print(f"💾 [SQL Saved] State: {state_name} | Spent: {round(duration_sec, 2)}s")
    except Exception as e:
        print(f"❌ DB Error: {e}")

def log_cycle_complete_to_sql(cycle_number, total_duration_sec):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("""
            INSERT INTO production_summary (end_timestamp, cycle_number, total_duration_sec)
            VALUES (?, ?, ?)
        """, (now_str, cycle_number, round(total_duration_sec, 2)))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"❌ DB Error: {e}")

# ==================================================
# 🤖 PREDICTIVE AI MODEL
# ==================================================
def evaluate_predictive_ai(step_name, current_duration):
    history = step_history_cache[step_name]
    if len(history) < 3:
        history.append(current_duration)
        return False
    mean_val = sum(history) / len(history)
    variance = sum((x - mean_val) ** 2 for x in history) / len(history)
    std_dev = math.sqrt(variance)
    is_anomaly = current_duration > (mean_val + 1.5 * std_dev) or current_duration > 1.35
    history.append(current_duration)
    if len(history) > 5:
        history.pop(0)
    return is_anomaly

# ==================================================
# 🧠 FSM MAIN BRAIN LOOP
# ==================================================
def fsm_brain_loop():
    global system_data, trigger_reset_timer
    state_index = STATES.index('READY') 
    last_transition_time = time.time()
    cycle_start_time = time.time()
    
    durations = {
        'LOAD_CARRIER': 0.5, 'INDEX_CARRIER': 0.8, 'POWER_ON': 0.5, 'SET_PARAMS': 0.5,
        'SENSOR_CHECK_CARRIER': 0.4, 'READY': 0.5, 'LOAD_PART': 0.8, 'VISION': 0.4,
        'CHECK_TEMP': 0.4, 'FEED_CARRIER': 1.0, 'COUNT_PROCESS': 0.5, 'COUNT_CHECK': 0.5,
        'COUNT_ACCUMULATE': 0.4, 'SEAL_PROCESS': 1.0, 'VISION_QC': 0.4, 'TAKEUP_REEL': 1.0,
        'ALARM': 0.5
    }
    checkpoints = ['SENSOR_CHECK_CARRIER', 'VISION', 'CHECK_TEMP', 'COUNT_CHECK', 'VISION_QC']

    while True:
        try:
            now = time.time()
            if trigger_reset_timer:
                state_index = STATES.index(system_data["current_state"])
                last_transition_time = now
                cycle_start_time = now
                trigger_reset_timer = False

            curr = STATES[state_index]
            system_data["current_state"] = curr

            if system_data["running"] and curr != 'ALARM':
                simulated_delay = 0.0
                if curr in ["FEED_CARRIER", "SEAL_PROCESS"] and system_data["cycles"] in [3, 4]:
                    simulated_delay = 0.55  

                target_dur = (durations.get(curr, 0.5) + simulated_delay) / system_data["speed_mul"]
                
                if curr in checkpoints and system_data["mode"] == "semi" and not system_data["step_allowed"] and (now - last_transition_time >= target_dur):
                    system_data["step_allowed"] = True
                
                elif not system_data["step_allowed"] and (now - last_transition_time >= target_dur):
                    actual_spent = now - last_transition_time
                    
                    if curr in ["FEED_CARRIER", "SEAL_PROCESS"]:
                        has_anomaly = evaluate_predictive_ai(curr, actual_spent)
                        if has_anomaly:
                            system_data["predictive_warning"] = f"⚠️ [AI WARNING]: Detected Friction at {curr} ({round(actual_spent, 2)}s)!"
                        else:
                            if curr == 'SEAL_PROCESS' and actual_spent < 1.3:
                                system_data["predictive_warning"] = ""

                    log_state_transition_to_sql(curr, actual_spent, system_data["mode"], system_data["cycles"])

                    if curr == 'FEED_CARRIER':
                        system_data["camera1_count"] += 1
                        system_data["encoder_count"] += 1
                        state_index = STATES.index('COUNT_PROCESS')
                    elif curr == 'COUNT_CHECK':
                        if system_data["camera1_count"] == system_data["encoder_count"]:
                            state_index = STATES.index('COUNT_ACCUMULATE')
                        else:
                            state_index = STATES.index('ALARM')
                    elif curr == 'COUNT_ACCUMULATE':
                        system_data["pieces_count"] += 1
                        state_index = STATES.index('SEAL_PROCESS')
                    elif curr == 'TAKEUP_REEL':
                        total_cycle_time = now - cycle_start_time
                        system_data["cycles"] += 1
                        log_cycle_complete_to_sql(system_data["cycles"], total_cycle_time)
                        system_data["camera1_count"] = 0
                        system_data["encoder_count"] = 0
                        system_data["pieces_count"] = 0
                        cycle_start_time = now
                        state_index = STATES.index('SENSOR_CHECK_CARRIER')
                    else:
                        state_index = (state_index + 1) % len(STATES)

                    last_transition_time = now

            # IO MAPPING บิตคุมกระบอกสูบและมอเตอร์
            OP0 = 0x00
            if system_data["running"]:
                if curr == 'INDEX_CARRIER':  OP0 |= 0x01  
                elif curr == 'FEED_CARRIER': OP0 |= 0x10  
                elif curr == 'SEAL_PROCESS': OP0 |= 0x04  
                elif curr == 'TAKEUP_REEL':  OP0 |= 0x02  
                elif curr == 'ALARM':        OP0 |= 0x08
            system_data["op0"] = OP0

            # 🚀 ยิงบิตสดผ่านท่อ Socket ค้างสายข้ามเลเยอร์ไปหา Rust ประมวลผลเครือข่าย
            actual_hardware_ip0 = send_data_to_rust_layer(
                system_data["current_state"], 
                system_data["running"], 
                system_data["op0"], 
                system_data["ip0"], 
                system_data["cycles"]
            )

            time.sleep(LOOP_DELAY)
        except Exception as e:
            time.sleep(0.5)

# ==================================================
# 🌐 WEBSOCKET BRIDGE SERVER
# ==================================================
async def ws_handler(websocket):
    global system_data, trigger_reset_timer
    connected_clients.add(websocket)
    try:
        async for message in websocket:
            data = json.loads(message)
            action = data.get("action")
            if action == "START":
                system_data["running"] = True
                system_data["predictive_warning"] = ""
                trigger_reset_timer = True
            elif action == "RESET":
                system_data["running"] = False
                system_data["cycles"] = 0
                system_data["current_state"] = "READY"
                system_data["predictive_warning"] = ""
                trigger_reset_timer = True
            elif action == "MODE":
                system_data["mode"] = data.get("mode", "auto")
            elif action == "SPEED":
                system_data["speed_mul"] = float(data.get("value", 1.0))
            elif action == "GET_HISTORY":
                try:
                    conn = sqlite3.connect(DB_FILE)
                    cursor = conn.cursor()
                    cursor.execute("SELECT state_name, duration_sec, current_cycle, control_mode, timestamp FROM machine_logs ORDER BY id DESC LIMIT 100")
                    rows = cursor.fetchall()
                    conn.close()
                    await websocket.send(json.dumps({
                        "type": "HISTORY_RESPONSE",
                        "data": [{"state": r[0], "duration": r[1], "cycle": r[2], "mode": r[3], "time": r[4]} for r in reversed(rows)]
                    }))
                except: pass
            elif action == "DECISION":
                if system_data["step_allowed"]:
                    system_data["step_allowed"] = False
                    trigger_reset_timer = True
                    if data.get("value", True):
                        state_idx = STATES.index(system_data["current_state"])
                        system_data["current_state"] = STATES[(state_idx + 1) % len(STATES)]
                    else:
                        system_data["current_state"] = "ALARM"
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        connected_clients.remove(websocket)

async def broadcast_state():
    while True:
        if connected_clients:
            try:
                msg = json.dumps({"type": "LIVE_SYNC", "system": system_data})
                await asyncio.gather(*[client.send(msg) for client in connected_clients], return_exceptions=True)
            except: pass
        await asyncio.sleep(0.05) 

async def main_async():
    init_db()
    threading.Thread(target=fsm_brain_loop, daemon=True).start()
    asyncio.create_task(broadcast_state())
    async with websockets.serve(ws_handler, "0.0.0.0", 8765):
        await asyncio.Event().wait() 

if __name__ == "__main__":
    asyncio.run(main_async())