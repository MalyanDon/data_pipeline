"""
Mistral 7B LLM API Server for SmartGov Ex-Gratia Chatbot
Provides natural language processing capabilities for intent recognition
"""
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import logging
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Mistral 7B API Server", version="1.0.0")

class GenerateRequest(BaseModel):
    prompt: str
    max_tokens: int = 150
    temperature: float = 0.7
    top_p: float = 0.9

class GenerateResponse(BaseModel):
    generated_text: str
    status: str = "success"

class MistralServer:
    def __init__(self):
        self.model = None
        self.tokenizer = None
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        logger.info(f"Using device: {self.device}")
        
    def load_model(self):
        """Load open instruction-tuned model with optimizations"""
        try:
            # Using an open model that doesn't require authentication
            model_name = "microsoft/DialoGPT-medium"  # Alternative: "distilgpt2" for faster loading
            
            # Configure quantization for GPU memory efficiency
            if self.device == "cuda":
                bnb_config = BitsAndBytesConfig(
                    load_in_4bit=True,
                    bnb_4bit_use_double_quant=True,
                    bnb_4bit_quant_type="nf4",
                    bnb_4bit_compute_dtype=torch.bfloat16
                )
                
                self.model = AutoModelForCausalLM.from_pretrained(
                    model_name,
                    quantization_config=bnb_config,
                    device_map="auto",
                    trust_remote_code=True
                )
            else:
                # CPU mode
                self.model = AutoModelForCausalLM.from_pretrained(
                    model_name,
                    torch_dtype=torch.float32,
                    device_map="cpu",
                    trust_remote_code=True
                )
            
            self.tokenizer = AutoTokenizer.from_pretrained(model_name)
            if self.tokenizer.pad_token is None:
                self.tokenizer.pad_token = self.tokenizer.eos_token
                
            logger.info("✅ Mistral 7B model loaded successfully")
            
        except Exception as e:
            logger.error(f"❌ Error loading Mistral model: {e}")
            raise HTTPException(status_code=500, detail=f"Model loading failed: {e}")
    
    def generate_response(self, prompt: str, max_tokens: int = 150, 
                         temperature: float = 0.7, top_p: float = 0.9) -> str:
        """Generate response using Mistral 7B"""
        try:
            if self.model is None or self.tokenizer is None:
                raise HTTPException(status_code=500, detail="Model not loaded")
            
            # Format prompt for Mistral
            formatted_prompt = f"<s>[INST] {prompt} [/INST]"
            
            # Tokenize input
            inputs = self.tokenizer(
                formatted_prompt, 
                return_tensors="pt", 
                truncation=True, 
                max_length=512
            ).to(self.device)
            
            # Generate response
            with torch.no_grad():
                outputs = self.model.generate(
                    **inputs,
                    max_new_tokens=max_tokens,
                    temperature=temperature,
                    top_p=top_p,
                    do_sample=True,
                    pad_token_id=self.tokenizer.eos_token_id,
                    eos_token_id=self.tokenizer.eos_token_id
                )
            
            # Decode response
            generated_text = self.tokenizer.decode(
                outputs[0][inputs.input_ids.shape[1]:], 
                skip_special_tokens=True
            ).strip()
            
            return generated_text
            
        except Exception as e:
            logger.error(f"❌ Generation error: {e}")
            raise HTTPException(status_code=500, detail=f"Generation failed: {e}")

# Initialize Mistral server
mistral_server = MistralServer()

@app.on_event("startup")
async def startup_event():
    """Load model on startup"""
    logger.info("🚀 Starting Mistral 7B API Server...")
    mistral_server.load_model()
    logger.info("✅ Server ready to accept requests")

@app.get("/")
async def root():
    """Health check endpoint"""
    return {"status": "Mistral 7B API Server is running", "device": mistral_server.device}

@app.get("/health")
async def health_check():
    """Detailed health check"""
    model_loaded = mistral_server.model is not None
    return {
        "status": "healthy" if model_loaded else "unhealthy",
        "model_loaded": model_loaded,
        "device": mistral_server.device,
        "cuda_available": torch.cuda.is_available()
    }

@app.post("/generate", response_model=GenerateResponse)
async def generate_text(request: GenerateRequest):
    """Generate text using Mistral 7B"""
    try:
        generated_text = mistral_server.generate_response(
            prompt=request.prompt,
            max_tokens=request.max_tokens,
            temperature=request.temperature,
            top_p=request.top_p
        )
        
        return GenerateResponse(
            generated_text=generated_text,
            status="success"
        )
        
    except Exception as e:
        logger.error(f"❌ API Generation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    print("🤖 Starting Mistral 7B API Server...")
    print("📋 Endpoints:")
    print("   • GET  /          - Health check")
    print("   • GET  /health    - Detailed health")
    print("   • POST /generate  - Text generation")
    print("💡 Access docs at: http://localhost:8000/docs")
    
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000,
        log_level="info"
    ) 