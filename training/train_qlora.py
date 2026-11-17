#!/usr/bin/env python3
"""
Standalone QLoRA Fine-Tuning Script for kube-doctor-1.35-ocp-4.22
Uses Hugging Face Transformers + PEFT + TRL (SFTTrainer).

Usage:
  python3 -m venv .venv
  source .venv/bin/activate
  pip install torch transformers peft trl datasets bitsandbytes accelerate
  python3 training/train_qlora.py
"""

import argparse
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Fine-tune kube-doctor-1.35-ocp-4.22 with QLoRA (PEFT + TRL)")
    parser.add_argument("--base-model", default="Qwen/Qwen2.5-Coder-7B-Instruct", help="Base HuggingFace model ID")
    parser.add_argument(
        "--dataset",
        default=str(Path(__file__).parent / "data" / "kube_doctor_k8s135_ocp422_sft.jsonl"),
        help="Path to generated JSONL dataset",
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).parent / "checkpoints" / "kube-doctor-1.35-ocp-4.22-qlora"),
        help="Output directory for LoRA adapter weights",
    )
    parser.add_argument("--epochs", type=int, default=3)
    args = parser.parse_args()

    try:
        import torch
        from datasets import load_dataset
        from peft import LoraConfig
        from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig
        from trl import SFTConfig, SFTTrainer
    except ImportError as exc:
        print("Missing Python ML dependencies for local GPU QLoRA training:", exc)
        print("\nInstall them in a virtual environment with:")
        print("  python3 -m venv .venv && source .venv/bin/activate")
        print("  pip install torch transformers peft trl datasets bitsandbytes accelerate")
        print("\nOr run Axolotl via its official Docker image (not the Ubuntu snap):")
        print(
            "  docker run --gpus all -v $(pwd):/workspace winglian/axolotl:main-latest "
            "accelerate launch -m axolotl.cli.train /workspace/training/qlora_config.yaml"
        )
        raise SystemExit(1)

    print(f"Loading dataset from {args.dataset}...")
    dataset = load_dataset("json", data_files=args.dataset, split="train")

    bnb_config = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.bfloat16 if torch.cuda.is_available() else torch.float32,
        bnb_4bit_use_double_quant=True,
    )

    tokenizer = AutoTokenizer.from_pretrained(args.base_model, trust_remote_code=True)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    model = AutoModelForCausalLM.from_pretrained(
        args.base_model,
        quantization_config=bnb_config if torch.cuda.is_available() else None,
        device_map="auto" if torch.cuda.is_available() else None,
        trust_remote_code=True,
    )

    peft_config = LoraConfig(
        r=32,
        lora_alpha=64,
        lora_dropout=0.05,
        bias="none",
        task_type="CAUSAL_LM",
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"],
    )

    sft_config = SFTConfig(
        output_dir=args.output_dir,
        num_train_epochs=args.epochs,
        per_device_train_batch_size=1,
        gradient_accumulation_steps=4,
        learning_rate=2e-4,
        logging_steps=1,
        save_strategy="epoch",
        bf16=torch.cuda.is_available() and torch.cuda.is_bf16_supported(),
        max_seq_length=4096,
    )

    trainer = SFTTrainer(
        model=model,
        train_dataset=dataset,
        peft_config=peft_config,
        processing_class=tokenizer,
        args=sft_config,
    )

    print("Starting QLoRA fine-tuning...")
    trainer.train()
    trainer.save_model(args.output_dir)
    tokenizer.save_pretrained(args.output_dir)
    print(f"Saved fine-tuned QLoRA adapter to {args.output_dir}")


if __name__ == "__main__":
    main()

