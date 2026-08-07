import os
import torch
from dotenv import load_dotenv
from datasets import load_dataset
from peft import LoraConfig
from trl import SFTConfig , SFTTrainer
from transformers import BitsAndBytesConfig, AutoModelForCausalLM,AutoTokenizer,EarlyStoppingCallback,AutoTokenizer
load_dotenv()
dataset_name = os.getenv("dataset_name")
dataset = load_dataset(dataset_name)

def format_data(example):
    text = example['display_text']
    text = text.replace("<|user|>", "<|im_start|>user")
    text = text.replace("<|end|>", "<|im_end|>")
    return {"text": text}

dataset = dataset['train'].map(format_data,num_proc=8,remove_columns=dataset['train'].column_names)
dataset = dataset.train_test_split(test_size=0.2)
train_dataset = dataset['train']
eval_dataset = dataset['test']
bnb_config = BitsAndBytesConfig(os.getenv("bnb_config"))
model_name = os.getenv("model_name")
model = AutoModelForCausalLM.from_pretrained(
    model_name,
    quantization_config = bnb_config,
    device_map = "cuda:0",
    trust_remote_code = True
)
lora_config = LoraConfig(
    r=16,
    lora_alpha=32,
    lora_dropout=0.05,
    bias="none",
    target_modules=["q_proj", "v_proj", "k_proj", "o_proj"],
    task_type = "CAUSAL_LM"
)
# Fixed Training Arguments
args = SFTConfig(
    output_dir="./results",
    num_train_epochs=3,
    per_device_train_batch_size=4,
    gradient_accumulation_steps=4,
    optim="adamw_torch", # use "adamw_torch" or "paged_adamw_8bit"
    save_steps=10,
    logging_steps=10,
    learning_rate=2e-4,
    weight_decay=0.001,
    fp16=False,
    bf16=False,
    max_grad_norm=0.3,
    max_steps=-1,
    lr_scheduler_type="cosine",
    eval_strategy="steps",
    save_strategy="steps",
    eval_steps=10,
    load_best_model_at_end=True,
    metric_for_best_model="eval_loss",
    greater_is_better=False,
    save_total_limit=3,
    report_to="tensorboard",
    dataset_text_field="text"
)
trainer = SFTTrainer(
    model=model,
    args=args,
    train_dataset=train_dataset,
    eval_dataset=eval_dataset,
    peft_config=lora_config,
    callbacks=[EarlyStoppingCallback(early_stopping_patience=3)]
)
# Merge and save
model = trainer.model.merge_and_unload()
model.save_pretrained("./tinyllama")
