# -*- coding: utf-8 -*-
"""
Created on Tue Jun 24 14:42:47 2025

@author: Pongo
"""

from datasets import load_dataset

dataset_id = "McAuley-Lab/Amazon-Reviews-2023"
target_cfg = "raw_review_Subscription_Boxes"

print(f"→ Downloading config: {target_cfg}")
ds = load_dataset(
    dataset_id,
    target_cfg,
    trust_remote_code=True,        # allow custom loading script
    cache_dir="C:/Users/Pongo/Downloads/MDIK_Tugas_2"
)

# e.g. save each split locally, if you like:
# ds["train"].to_csv(f"amazon_reviews_{target_cfg}_train.csv", index=False)
