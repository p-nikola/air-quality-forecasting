from pathlib import Path
from chronos import Chronos2Pipeline
import torch

target_dir = Path("chronos2_zero_shot")

pipeline = Chronos2Pipeline.from_pretrained(
    "amazon/chronos-2",
    device_map="auto",
    dtype=torch.bfloat16,
)

pipeline.save_pretrained(target_dir)
print(f"Saved zero-shot model to {target_dir.resolve()}")
