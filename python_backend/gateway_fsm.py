import asyncio
import json
import sqlite3
import time
from datetime import datetime

# ==================================================
# ⚙️ CONFIGURATION & NETWORK SETTINGS
# ==================================================
import websockets

RUST_BRIDGE_HOST = "127.0.0.1"
RUST_BRIDGE_PORT = 8766
DB_FILE = "production_data.db"

# 16-State ลอจิกการทำงานของนิว
STATES = [
    "LOAD_CARRIER", "INDEX_CARRIER", "POWER_ON", "SET_PARAMS",
    "SENSOR_CHECK_CARRIER", "READY", "LOAD_PART", "VISION",
    "CHECK_TEMP", "FEED_CARRIER", "COUNT_PROCESS", "COUNT_CHECK",
    "COUNT_ACCUMULATE", "SEAL_PROCESS", "VISION_QC", "TAKEUP_REEL", "ALARM"
]

# Shared Global State
system_state = {
    "current_state": "READY",
    "cycles": 0,
    "mode": "auto",       # auto / semi
    "running": False,
    "step_allowed": False,
    "speed": 1.0,
    "ip0": 0,             # Input bits
    "op0": 0,             # Output bits
    "predictive_warning": "",
    "state_start_time": time.time()
}

# ==================================================
# 🗄️ DATABASE SYSTEM (SQLite Setup)
# ==================================================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # สร้างตารางเก็บประวัติรายสเตป (สำหรับประมวลผลคอขวด)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS state_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cycle INTEGER,
            state TEXT,
            duration REAL,
            timestamp TEXT
        )
    """)
    # สร้างตารางสรุปเวลาของแต่ละ Cycle (สำหรับกราฟ OEE)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cycle_summaries (
            cycle INTEGER PRIMARY KEY,
            total_duration REAL,
            timestamp TEXT
        )
    """)
    conn.commit()
    conn.close()

def log_state_to_db(cycle, state, duration):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute(
            "INSERT INTO state_logs (cycle, state, duration, timestamp) VALUES (?, ?, ?, ?)",
            (cycle, state, duration, now)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"❌ DB Error: {e}")

# ==================================================
# 📊 ANALYTICS QUERY ENGINE (ท่อข้อมูลดึงไปหน้ากราฟ)
# ==================================================
def get_analytics_data():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # 1. ดึงข้อมูลระยะเวลาของแต่ละ Cycle (กราฟเส้นประวัติราย Cycle)
        # ดึง 20 Cycle ล่าสุดมาแสดงแนวโน้ม OEE Efficiency
        cursor.execute("""
            SELECT cycle, SUM(duration) as total_dur 
            FROM state_logs 
            GROUP BY cycle 
            ORDER BY cycle DESC LIMIT 20
        """)
        rows_cycles = cursor.fetchall()
        cycles_data = [{"cycle": r[0], "duration": round(r[1], 2)} for r in reversed(rows_cycles)]
        
        # 2. คำนวณหาค่าเฉลี่ยเวลาในแต่ละสเตป (กราฟแท่ง Bottleneck Analysis)
        cursor.execute("""
            SELECT state, AVG(duration) as avg_dur 
            FROM state_logs 
            GROUP BY state
            ORDER BY avg_dur DESC
        """)
        rows_states = cursor.fetchall()
        states_avg = [{"state": r[0], "avg_duration": round(r[1], 2)} for r in rows_states]
        
        conn.close()
        return {"cycles_data": cycles_data, "states_avg": states_avg}
    except Exception as e:
        print(f"❌ Analytics Fetch Error: {e}")
        return {"cycles_data": [], "states_avg": []}

def get_raw_history():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT cycle, state, duration, timestamp FROM state_logs ORDER BY id DESC LIMIT 100")
        rows = cursor.fetchall()
        conn.close()
        return [{"cycle": r[0], "state": r[1], "duration": r[2], "time": r[3]} for r in reversed(rows)]
    except Exception as e:
        return []

# ==================================================
# 🤖 FSM LOGIC MACHINE (ตรรกะคำนวณสเตป 16 ขั้นของนิว)
# ==================================================
def change_state(new_state):
    now = time.time()
    duration = now - system_state["state_start_time"]
    
    # บันทึกสถานะเก่าลงฐานข้อมูล SQL ก่อนย้ายสเตป
    if system_state["running"] and system_state["cycles"] > 0:
        log_state_to_db(system_state["cycles"], system_state["current_state"], duration)
        
    print(f"🔄 FSM State Change: {system_state['current_state']} -> {new_state} (Spent: {duration:.2f}s)")
    system_state["current_state"] = new_state
    system_state["state_start_time"] = now
    
    # กำหนด Output Bits (OP) ไปคุมรีเลย์บนบอร์ดจริงจำลองตามสเตป
    if new_state == "READY": system_state["op0"] = 0x01
    elif new_state == "LOAD_PART": system_state["op0"] = 0x02
    elif new_state == "SEAL_PROCESS": system_state["op0"] = 0x04
    elif new_state == "FEED_CARRIER": system_state["op0"] = 0x08
    elif new_state == "TAKEUP_REEL": system_state["op0"] = 0x10
    else: system_state["op0"] = 0x00

async def fsm_loop():
    while True:
        if not system_state["running"]:
            await asyncio.sleep(0.5)
            continue
            
        curr = system_state["current_state"]
        S = system_state["speed"]
        
        # เช็คเงื่อนไขโหมดการทำงาน
        is_inspect_state = curr in ["SENSOR_CHECK_CARRIER", "VISION", "CHECK_TEMP", "COUNT_CHECK", "VISION_QC"]
        
        if system_state["mode"] == "semi" and is_inspect_state and not system_state["step_allowed"]:
            # ถ้าอยู่ในโหมด Semi-Auto และถึงจุดตรวจสอบ ให้หยุดรอการกด OK/NG จากหน้าเว็บ
            await asyncio.sleep(0.1)
            continue

        # ลูปกลไกสเตปการทำงานอัตโนมัติ
        await asyncio.sleep(1.0 / S) # ขยับความเร็วตาม Slider หน้าเว็บ
        
        if curr == "READY":
            system_state["cycles"] += 1
            change_state("LOAD_PART")
        elif curr == "LOAD_PART":
            change_state("VISION")
            system_state["step_allowed"] = False
        elif curr == "VISION":
            change_state("CHECK_TEMP")
            system_state["step_allowed"] = False
        elif curr == "CHECK_TEMP":
            change_state("FEED_CARRIER")
        elif curr == "FEED_CARRIER":
            change_state("COUNT_PROCESS")
        elif curr == "COUNT_PROCESS":
            change_state("COUNT_CHECK")
            system_state["step_allowed"] = False
        elif curr == "COUNT_CHECK":
            change_state("COUNT_ACCUMULATE")
        elif curr == "COUNT_ACCUMULATE":
            change_state("SEAL_PROCESS")
        elif curr == "SEAL_PROCESS":
            change_state("VISION_QC")
            system_state["step_allowed"] = False
        elif curr == "VISION_QC":
            change_state("TAKEUP_REEL")
        elif curr == "TAKEUP_REEL":
            change_state("READY")

# ==================================================
# 🌐 WEBSOCKET SERVER INTERFACE (ท่อแจกจ่ายข้อมูล)
# ==================================================
connected_clients = set()

async def ws_handler(websocket):
    connected_clients.add(websocket)
    try:
        async for message in websocket:
            data = json.loads(message)
            action = data.get("action")
            
            if action == "START":
                system_state["running"] = True
                system_state["state_start_time"] = time.time()
                print("▶️ System Started via Dashboard")
                
            elif action == "RESET":
                system_state["running"] = False
                system_state["current_state"] = "READY"
                system_state["cycles"] = 0
                system_state["op0"] = 0
                print("↺ System Reset via Dashboard")
                
            elif action == "MODE":
                system_state["mode"] = data.get("mode", "auto")
                system_state["step_allowed"] = False
                print(f"🕹️ Mode switched to: {system_state['mode']}")
                
            elif action == "SPEED":
                system_state["speed"] = float(data.get("value", 1.0))
                
            elif action == "DECISION":
                # รับสัญญาณการตัดสินใจคิว Semi-Auto (👍 OK / 👎 NG)
                passed = data.get("value", True)
                if passed:
                    system_state["step_allowed"] = True
                    print("👍 Inspection PASSED")
                else:
                    system_state["current_state"] = "ALARM"
                    system_state["running"] = False
                    print("👎 Inspection FAILED! System Brake Triggered!")
            
            # 🚨🎯 จุดแก้ไขสำคัญ: เมื่อหน้าจอ Analytics ร้องขอข้อมูลสถิติ
            elif action == "GET_ANALYTICS":
                analytics_res = get_analytics_data()
                response = {
                    "type": "ANALYTICS_RESPONSE",
                    "data": analytics_res
                }
                await websocket.send(json.dumps(response))
                print("📊 Sent Analytics SQLite Data to dashboard")
                
            elif action == "GET_HISTORY":
                history_res = get_raw_history()
                await websocket.send(json.dumps({"type": "HISTORY_RESPONSE", "data": history_res}))
                
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        connected_clients.remove(websocket)

async def broadcast_live_sync():
    while True:
        if connected_clients:
            payload = json.dumps({
                "type": "LIVE_SYNC",
                "system": system_state
            })
            await asyncio.gather(*[client.send(payload) for client in connected_clients], return_exceptions=True)
        await asyncio.sleep(0.1) # ส่งสตรีมหาหน้าจอและบอร์ดทุก 100ms

# ==================================================
# 🦀 RUST BRIDGE TCP CONNECTOR (ท่อสายแลนบอร์ดจริง)
# ==================================================
async def rust_bridge_client():
    while True:
        try:
            reader, writer = await asyncio.open_connection(RUST_BRIDGE_HOST, RUST_BRIDGE_PORT)
            print("💡 [Python -> Rust] Persistent Connection Established!")
            
            while True:
                # 1. ยิงสถานะปัจจุบันคุมบอร์ดส่งเข้าทางท่อ Rust
                tx_data = {
                    "state": system_state["current_state"],
                    "op0": system_state["op0"],
                    "cycles": system_state["cycles"]
                }
                writer.write((json.dumps(tx_data) + "\n").encode())
                await writer.drain()
                
                # 2. อ่านข้อมูลอินพุตสะท้อนกลับจากปุ่มกดบนบอร์ดจริง
                line = await reader.readline()
                if not line:
                    break
                    
                rx_data = json.loads(line.decode().strip())
                system_state["ip0"] = rx_data.get("ip0", 0)
                
                await asyncio.sleep(0.05) # ความถี่สแกนสัญญาณบอร์ด 50ms
                
        except Exception as e:
            print(f"⏳ Waiting for Rust Bridge socket layer... ({e})")
            await asyncio.sleep(2.0)

# ==================================================
# 🏁 MAIN ASYNC EXECUTOR
# ==================================================
async def main():
    init_db()
    print("🗄️ SQLite Engine Sync Check Complete.")
    print("🚀 Central Python Gateway Starting on port 8765...")
    
    # รันทั้ง 4 งานแบบขนาน (Concurrently) เจนเนอเรชันสมบูรณ์
    await asyncio.gather(
        websockets.serve(ws_handler, "0.0.0.0", 8765),
        broadcast_live_sync(),
        fsm_loop(),
        rust_bridge_client()
    )

if __name__ == "__main__":
    asyncio.run(main())
