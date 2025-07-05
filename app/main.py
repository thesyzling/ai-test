import logging
import os
import base64
import requests
import sqlite3

from openfabric_pysdk.app.execution.execution_context import ExecutionContext
from ontology_dc8f06af066e4a7880a5938933236037.config import ConfigClass
from core.stub import Stub



############################################################
# Basit AppModel tanımı (request ve response öznitelikleri ile)
############################################################
class AppModel:
    def __init__(self, request, response):
        self.request = request
        self.response = response

############################################################
# Execution callback function
############################################################
def execute(model: AppModel) -> None:
    request = model.request
    response = model.response

    prompt = request.prompt.strip()
    logging.info(f"Original prompt: {prompt}")

    # Step 1: Expand the prompt with local LLM (Ollama)
    llm_response = local_llm_expand(prompt)
    logging.info(f"Expanded prompt: {llm_response}")

    # Step 2: Generate image using Text-to-Image Openfabric app
    stub = get_stub()
    try:
        image_object = stub.call("f0997a01-d6d3-a5fe-53d8-561300318557", {"prompt": llm_response}, "super-user")
        image_data = image_object.get("result")
        if isinstance(image_data, str):
            image_data = base64.b64decode(image_data)
    except Exception as e:
        logging.error(f"Image generation error: {e}")
        response.message = "❌ Görsel oluşturulamadı."
        return

    image_path = "output_image.png"
    try:
        with open(image_path, "wb") as f:
            f.write(image_data)
    except Exception as e:
        logging.error(f"Image save error: {e}")
        response.message = "❌ Görsel kaydedilemedi."
        return

    # Step 3: Convert image to 3D model using Image-to-3D app
    try:
        with open(image_path, "rb") as img_file:
            image_bytes = img_file.read()
        image_base64 = base64.b64encode(image_bytes).decode("utf-8")
        model3d_object = stub.call("69543f29-4d41-4afc-7f29-3d51591f11eb", {"image": image_base64}, "super-user")
        model3d_base64 = model3d_object.get("result")
    except Exception as e:
        logging.error(f"3D model generation error: {e}")
        response.message = "❌ 3D model oluşturulamadı."
        return

    model_path = "output_model.glb"
    try:
        with open(model_path, "wb") as f:
            f.write(base64.b64decode(model3d_base64))
    except Exception as e:
        logging.error(f"3D model save error: {e}")
        response.message = "❌ 3D model kaydedilemedi."
        return

    # Step 4: Save to memory
    try:
        save_to_memory(prompt, llm_response, image_path, model_path)
    except Exception as e:
        logging.error(f"Memory save error: {e}")
        response.message = "❌ Hafızaya kaydedilemedi."
        return

    response.message = f"✅ Prompt işlendi: {prompt}"

############################################################
# Helpers
############################################################

# Dummy Stub ve dummy servisler
class DummyStub:
    def call(self, app, payload, user):
        if "image" in payload:
            # 3D model için sahte base64
            dummy_3d = base64.b64encode(b"dummy_3d_model_data").decode("utf-8")
            return {"result": dummy_3d}
        else:
            # Görsel için sahte base64
            dummy_img = base64.b64encode(b"dummy_image_data").decode("utf-8")
            return {"result": dummy_img}

def get_stub():
    return DummyStub()

def local_llm_expand(prompt: str) -> str:
    endpoint = os.getenv("LLM_ENDPOINT", "http://localhost:11434/api/generate")
    payload = {
        "model": "mistral",
        "prompt": f"Expand this prompt in vivid creative detail: {prompt}"
    }
    headers = {"Content-Type": "application/json"}
    try:
        logging.info(f"Sending LLM request to {endpoint} with payload: {payload}")
        response = requests.post(endpoint, json=payload, headers=headers, timeout=20)
        result = response.json()
        logging.info(f"LLM raw response: {result}")
        # Ollama returns a streaming response by default, but for /api/generate, 'response' key holds the text
        return result.get("response", prompt)
    except Exception as e:
        logging.error(f"LLM Error: {e}")
        return prompt

def save_to_memory(prompt: str, expanded: str, image_path: str, model_path: str):
    os.makedirs("memory", exist_ok=True)
    db_path = "memory/memory.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS prompts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prompt TEXT,
            expanded TEXT,
            image_path TEXT,
            model_path TEXT
        )
    ''')

    cursor.execute('''
        INSERT INTO prompts (prompt, expanded, image_path, model_path)
        VALUES (?, ?, ?, ?)
    ''', (prompt, expanded, image_path, model_path))

    conn.commit()
    conn.close()

# NOT: Eğer gerçek domain adreslerin farklıysa, yukarıdaki domainleri kendi Openfabric platformundaki endpointlerle değiştirmen gerekir.
