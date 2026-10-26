import asyncio
from typing import Dict, List, Set, Any

class Message:
    def __init__(self, msg_type: str, sender: str, payload: Any = None):
        self.msg_type = msg_type  # 'APP' or 'MARKER'
        self.sender = sender
        self.payload = payload

    def __repr__(self):
        return f"Msg({self.msg_type}, from={self.sender}, payload={self.payload})"

class Process:
    def __init__(self, pid: str, initial_balance: int):
        self.pid = pid
        self.balance = initial_balance
        self.incoming_channels: Dict[str, asyncio.Queue] = {}
        self.outgoing_channels: Dict[str, asyncio.Queue] = {}
        
        # Snapshot state tracking
        self.has_taken_snapshot = False
        self.saved_state = None
        self.channel_recording: Dict[str, bool] = {}
        self.recorded_channel_states: Dict[str, List[Message]] = {}
        self.markers_received: Set[str] = set()

    def add_channel(self, neighbor_pid: str, in_queue: asyncio.Queue, out_queue: asyncio.Queue):
        if in_queue:
            self.incoming_channels[neighbor_pid] = in_queue
            self.channel_recording[neighbor_pid] = False
            self.recorded_channel_states[neighbor_pid] = []
        if out_queue:
            self.outgoing_channels[neighbor_pid] = out_queue

    async def send_app_message(self, target_pid: str, amount: int):
        if self.balance >= amount:
            self.balance -= amount
            msg = Message(msg_type='APP', sender=self.pid, payload=amount)
            await self.outgoing_channels[target_pid].put(msg)
            print(f"[{self.pid}] Sent ${amount} to {target_pid}. New balance: ${self.balance}")

    async def initiate_snapshot(self):
        print(f"\n--- !!! Snapshot Initiated by {self.pid} !!! ---")
        self.has_taken_snapshot = True
        self.saved_state = self.balance
        
        # Start recording on all incoming channels
        for sender_id in self.incoming_channels:
            self.channel_recording[sender_id] = True

        # Send markers to all outgoing channels
        await self._broadcast_marker()

    async def _broadcast_marker(self):
        marker_msg = Message(msg_type='MARKER', sender=self.pid)
        for target_id, queue in self.outgoing_channels.items():
            await queue.put(marker_msg)

    async def handle_message(self, msg: Message):
        sender = msg.sender
        
        if msg.msg_type == 'MARKER':
            print(f"[{self.pid}] Received MARKER from {sender}")
            if not self.has_taken_snapshot:
                # First marker received
                self.has_taken_snapshot = True
                self.saved_state = self.balance
                self.recorded_channel_states[sender] = []  # Empty for sender
                self.channel_recording[sender] = False
                self.markers_received.add(sender)
                
                # Record all OTHER incoming channels
                for other_id in self.incoming_channels:
                    if other_id != sender:
                        self.channel_recording[other_id] = True
                
                # Broadcast marker forward
                await self._broadcast_marker()
            else:
                # Marker on already snapshotted process -> stop recording this channel
                self.channel_recording[sender] = False
                self.markers_received.add(sender)

        elif msg.msg_type == 'APP':
            if self.channel_recording.get(sender, False):
                self.recorded_channel_states[sender].append(msg)
            
            # Apply state update
            self.balance += msg.payload
            print(f"[{self.pid}] Received ${msg.payload} from {sender}. New balance: ${self.balance}")

    async def listen_channel(self, sender_id: str):
        queue = self.incoming_channels[sender_id]
        while True:
            msg = await queue.get()
            await self.handle_message(msg)
            queue.task_done()
            if len(self.markers_received) == len(self.incoming_channels):
                break

async def main():
    # Setup 3 connected processes P1, P2, P3
    p1, p2, p3 = Process('P1', 100), Process('P2', 200), Process('P3', 300)
    
    # FIFO Channels
    c12, c21 = asyncio.Queue(), asyncio.Queue()
    c23, c32 = asyncio.Queue(), asyncio.Queue()
    c31, c13 = asyncio.Queue(), asyncio.Queue()
    
    p1.add_channel('P2', c21, c12); p1.add_channel('P3', c31, c13)
    p2.add_channel('P1', c12, c21); p2.add_channel('P3', c32, c23)
    p3.add_channel('P1', c13, c31); p3.add_channel('P2', c23, c32)

    # Start Channel listeners
    tasks = []
    for p in [p1, p2, p3]:
        for sender in p.incoming_channels:
            tasks.append(asyncio.create_task(p.listen_channel(sender)))

    # Simulate transactions
    await p1.send_app_message('P2', 30)
    await p2.send_app_message('P3', 50)
    
    # Trigger snapshot at P1 while messages propagate
    await p1.initiate_snapshot()
    
    # Concurrent transactions during snapshot capture
    await p3.send_app_message('P1', 40)
    await p2.send_app_message('P1', 20)

    # Wait for completion
    await asyncio.sleep(1)

    print("\n--- GLOBAL SNAPSHOT RESULT ---")
    total_money = 0
    for p in [p1, p2, p3]:
        print(f"Process {p.pid} Saved Balance: ${p.saved_state}")
        total_money += p.saved_state
        for channel, msgs in p.recorded_channel_states.items():
            print(f"  Channel {channel} -> {p.pid} in-transit messages: {msgs}")
            for m in msgs:
                total_money += m.payload

    print(f"Total Captured System Value: ${total_money} (Initial sum was $600)")

if __name__ == "__main__":
    asyncio.run(main())
