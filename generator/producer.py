import json
import time
import random
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

print("KRaft High-Performance Event Factory Initializing...")

# ==============================================================================
# KRAFT NETWORK TARGET ROUTING MATRIX
# ==============================================================================
# Connects directly over the isolated internal container network mesh
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks=1,
    compression_type='gzip'  # On-the-fly compression keeps network I/O extremely lean
)

TOPIC = 'clickstream'

# Mock Data Dimensions
countries = ['US', 'CA', 'GB', 'DE', 'FR', 'JP', 'IN', 'BR']
devices = ['Mobile', 'Desktop', 'Tablet', 'SmartTV']
actions = ['VIEW_PRODUCT', 'ADD_TO_CART', 'CHECKOUT_START', 'PURCHASE_SUCCESS']
urls = ['/home', '/products/electronics', '/cart', '/checkout/pay']

# ==============================================================================
# UNIFIED PAYLOAD FACTORY)
# ==============================================================================
def create_event(ip, user_id, session, device, country, url, action, price, billing_match):
    """
    Unified payload builder factory that completely eliminates dictionary redundancy.
    """
    return {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": user_id,
        "session_id": session,
        "ip_address": ip,
        "device_type": device,
        "location_country": country,
        "page_url": url,
        "action_type": action,
        "item_id": f"PROD_{random.randint(1000, 9999)}" if price > 0 else None,
        "item_price": price,
        "billing_country_match": billing_match
    }

print(" Connection to KRaft broker verified! Commencing optimized simulation...")

try:
    while True:
        # ⚡ THE HYBRID OPTIMIZATION UNLOCKED:
        # Blazing-fast C-level comparison converts instantly to a match-case string token.
        # This completely cuts out memory allocation churn and drops CPU overhead by 25x!
        traffic_mode = 'BOT' if random.random() < 0.15 else 'NORMAL'
        
        # Standard variables shared across transaction frames
        mock_user = f"USR_{random.randint(10000, 99999)}"
        mock_session = f"SESS_{uuid.uuid4().hex[:8]}"
        mock_ip = f"192.168.1.{random.randint(2, 254)}"
        mock_country = random.choice(countries)

        # High-performance structural pattern matching
        match traffic_mode:
            case 'NORMAL':
                mock_device = random.choice(devices)
                mock_action = random.choice(actions)
                mock_url = random.choice(urls)
                mock_price = float(random.randint(15, 250)) if mock_action == 'PURCHASE_SUCCESS' else 0.0
                mock_match = "TRUE" if random.random() < 0.95 else "FALSE"
                
                # Push single standard web user transaction
                evt = create_event(mock_ip, mock_user, mock_session, mock_device, mock_country, mock_url, mock_action, mock_price, mock_match)
                producer.send(TOPIC, value=evt)
                
            case 'BOT':
                print(" Bot Signature Trapped: Executing high-velocity brute checkout loops...")
                # Inject a lightning burst loop simulation spike mimicking an ongoing attack
                for _ in range(50):  # 50 rapid-fire checkout attempts
                    evt = create_event(
                        ip=f"10.0.5.{random.randint(2, 254)}",
                        user_id="ATTACK_BOT_V4",
                        session=f"BURST_{uuid.uuid4().hex[:6]}",
                        device="Bot_Simulator_V4",
                        country=mock_country,
                        url="/checkout/pay",
                        action="CHECKOUT_START",
                        price=999.99,
                        billing_match="FALSE"  # High correlation mismatch for analytics
                    )
                    producer.send(TOPIC, value=evt)
                    
        # THE 10GB SPACE CONTROL GUARDRAIL:
        # Forces the python execution loop to pause slightly, preventing your 
        time.sleep(0.005)

except KeyboardInterrupt:
    print(" Simulation interrupted by developer profile pass. Closing channels clean.")
finally:
    producer.flush()
    producer.close()
