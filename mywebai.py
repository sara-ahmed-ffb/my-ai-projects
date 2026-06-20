import asyncio
import threading
from flask import Flask, request, jsonify, Response, stream_with_context
from flask_cors import CORS
from telethon import TelegramClient, events

app = Flask(__name__)
CORS(app)

API_ID = 11111111
API_HASH = 'YOUR_API_HASH_HERE'
PHONE_NUMBER = '+9647000000000'
BOT_USERNAME = '@YOUR_BOT_USERNAME'

telethon_loop = asyncio.new_event_loop()
client = TelegramClient('sara_claw_session', API_ID, API_HASH, loop=telethon_loop)

# لكل رسالة مرسلة: قائمة من الأجزاء الواصلة
pending_queues = {}

def extract_text_node(node):
    if not node or not isinstance(node, dict):
        return ''
    t = node.get('_', '')
    if t == 'TextPlain':
        return node.get('text', '')
    if t == 'TextConcat':
        return ''.join(extract_text_node(i) for i in node.get('texts', []))
    if t in ('TextBold', 'TextItalic', 'TextUnderline', 'TextStrike', 'TextUrl'):
        inner = extract_text_node(node.get('text', {}))
        return ('**' + inner + '**') if t == 'TextBold' else inner
    if 'text' in node:
        return extract_text_node(node['text'])
    if 'texts' in node:
        return ''.join(extract_text_node(i) for i in node['texts'])
    return ''

def extract_block(block):
    if not block or not isinstance(block, dict):
        return ''
    t = block.get('_', '')
    if t == 'PageBlockList':
        lines = []
        for item in block.get('items', []):
            txt = extract_text_node(item.get('text', {}))
            if txt.strip():
                lines.append('• ' + txt)
        return '\n'.join(lines)
    if 'text' in block:
        return extract_text_node(block['text'])
    return ''

def extract_rich(msg_dict):
    try:
        rich = msg_dict.get('rich_message')
        if not rich:
            return None
        parts = []
        for block in rich.get('blocks', []):
            t = extract_block(block)
            if t.strip():
                parts.append(t.strip())
        result = '\n\n'.join(parts)
        return result if result.strip() else None
    except:
        return None

def extract(msg):
    for field in [msg.text, msg.raw_text, msg.message]:
        if field and str(field).strip():
            return str(field).strip()
    try:
        return extract_rich(msg.to_dict())
    except:
        return None

def handle_msg(msg):
    text = extract(msg)
    for sent_id, q in list(pending_queues.items()):
        if msg.id > sent_id:
            if text:
                q.put(('chunk', text))
            break

@client.on(events.NewMessage(incoming=True))
async def on_new(event):
    handle_msg(event.message)

@client.on(events.MessageEdited(incoming=True))
async def on_edit(event):
    handle_msg(event.message)

import queue

@app.route('/api/chat', methods=['POST'])
def chat():
    user_data = request.json or {}
    user_message = user_data.get("message", "").strip()
    if not user_message:
        return jsonify({"reply": "الرسالة فارغة!"})

    q = queue.Queue()

    async def send_msg():
        sent = await client.send_message(BOT_USERNAME, user_message)
        pending_queues[sent.id] = q
        print(f"[DEBUG] sent_msg_id={sent.id}")

    future = asyncio.run_coroutine_threadsafe(send_msg(), telethon_loop)
    future.result(timeout=10)

    def generate():
        last_text = ''
        deadline = 90
        waited = 0
        while waited < deadline:
            try:
                kind, data = q.get(timeout=1)
                waited = 0
                if kind == 'chunk' and data != last_text:
                    last_text = data
                    # نرسل النص الكامل دائماً
                    safe = data.replace('\n', '\\n')
                    print(f"[STREAM] {len(data)} حرف")
                    yield f"data: {safe}\n\n"
            except queue.Empty:
                waited += 1
                if last_text and waited >= 4:
                    break

        for k, v in list(pending_queues.items()):
            if v is q:
                del pending_queues[k]
                break
        yield "data: [DONE]\n\n"

    return Response(stream_with_context(generate()), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

def run_telethon_bg():
    asyncio.set_event_loop(telethon_loop)
    print("[TG] جاري الاتصال...")
    client.start(phone=PHONE_NUMBER)
    print("[TG] جاهز!")
    telethon_loop.run_forever()

if __name__ == '__main__':
    bg_thread = threading.Thread(target=run_telethon_bg, daemon=True)
    bg_thread.start()
    import time
    time.sleep(3)
    app.run(port=5000, debug=False, use_reloader=False, threaded=True)