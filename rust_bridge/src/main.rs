use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct PythonSystemData {
    current_state: String,
    running: bool,
    op0: u8,
    ip0: u8,
    cycles: u32,
}

// พิกัด IP บอร์ดของนิวตามที่ตั้งไว้ในโค้ดตัวอย่างไฟกระพริบ
const STM32_IP_ADDR: &str = "192.168.0.100:502"; 
const PYTHON_BRIDGE_ADDR: &str = "127.0.0.1:8766";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🦀 [Rust Layer] Custom SCH-IO Socket Bridge Starting...");

    let socket_addr: SocketAddr = STM32_IP_ADDR.parse()?;
    
    // เชื่อมต่อบอร์ดด้วยท่อ Raw TCP Socket ความเร็วสูงตรงๆ
    let stm32_stream = match TcpStream::connect(socket_addr).await {
        Ok(stream) => {
            println!("✅ [Rust -> STM32] Connected to H7 Core Successfully at {}", STM32_IP_ADDR);
            Arc::new(Mutex::new(Some(stream)))
        }
        Err(e) => {
            println!("⚠️ [Rust -> STM32] Connect failed: ({}). Running in Loopback/Sim Mode.", e);
            Arc::new(Mutex::new(None))
        }
    };

    // ปล่อยเธรดแยกสำหรับคอยยิงคำสั่ง Heartbeat (*IDN?) หาบอร์ดในทุกๆ 1 วินาที
    let stm32_clone_for_hb = Arc::clone(&stm32_stream);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let mut guard = stm32_clone_for_hb.lock().await;
            if let Some(ref mut stream) = *guard {
                // ส่งคำสั่ง *IDN?\n ไปเช็คความพร้อมตามลอจิกบอร์ดของนิว
                if let Err(_) = stream.write_all(b"*IDN?\n").await {
                    println!("❌ [Heartbeat] STM32 disconnected.");
                } else {
                    // ดักอ่านสายอักษรที่ตอบกลับมาล้างบัฟเฟอร์
                    let mut hb_buf = [0; 64];
                    let _ = stream.read(&mut hb_buf).await;
                }
            }
        }
    });

    let listener = TcpListener::bind(PYTHON_BRIDGE_ADDR).await?;
    println!("🚀 [Rust Server] Listening for Python FSM Core on tcp://{}", PYTHON_BRIDGE_ADDR);

    loop {
        let (socket, _addr) = listener.accept().await?;
        let stm32_clone = Arc::clone(&stm32_stream);
        
        tokio::spawn(async move {
            if let Err(e) = handle_python_connection(socket, stm32_clone).await {
                println!("❌ [Rust Connection Thread] Error: {}", e);
            }
        });
    }
}

async fn handle_python_connection(
    mut stream: TcpStream, 
    stm32_client: Arc<Mutex<Option<TcpStream>>>
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = [0; 1024];
    loop {
        let bytes_read = stream.read(&mut buffer).await?;
        if bytes_read == 0 { break; }

        let raw_data = String::from_utf8_lossy(&buffer[..bytes_read]);
        for json_chunk in raw_data.split('\n') {
            if json_chunk.trim().is_empty() { continue; }

            if let Ok(sys_data) = serde_json::from_str::<PythonSystemData>(json_chunk) {
                let mut guard = stm32_client.lock().await;
                if let Some(ref mut stm32) = *guard {
                    
                    // 🦾 1. สั่งเอาต์พุตลงบอร์ด: ถอดแบบโครงสร้างจากฟังก์ชัน write_op ของนิวเป๊ะๆ
                    let mut write_pkt = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x01, 0x0F, 0x00, 0x40, 0x00, 0x08, 0x01, sys_data.op0];
                    if let Err(_) = stm32.write_all(&write_pkt).await { continue; }
                    let mut ack_buf = [0; 12];
                    let _ = stm32.read(&mut ack_buf).await; // อ่านล้างเฟรมตอบกลับ 12 ไบต์

                    // 🔍 2. อ่านเซนเซอร์กลับ: ถอดแบบโครงสร้างจากฟังก์ชัน read_ip ของนิวเป๊ะๆ
                    let read_pkt = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x01, 0x01, 0x00, 0x00, 0x00, 0x08];
                    if let Err(_) = stm32.write_all(&read_pkt).await { continue; }
                    let mut resp_buf = [0; 10];
                    
                    if let Ok(n) = stm32.read(&mut resp_buf).await {
                        if n >= 10 {
                            let ip0_byte = resp_buf[9]; // ดึงไบต์ที่ 9 ซึ่งเป็นข้อมูลเซนเซอร์จริงส่งกลับหา Python
                            let response = format!("{{\"ip0\":{}}}\n", ip0_byte);
                            let _ = stream.write_all(response.as_bytes()).await;
                            continue;
                        }
                    }
                }
                
                // โหมด Sim/Loopback สตรีมไฟนิ่งต่อเนื่องเมื่อไม่ได้ต่อบอร์ด
                let response = format!("{{\"ip0\":{}}}\n", sys_data.ip0);
                let _ = stream.write_all(response.as_bytes()).await;
            }
        }
    }
    Ok(())
}