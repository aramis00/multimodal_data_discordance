import pandas as pd



OUTPUT_DIR = Path("/mnt/s/OneDrive/Project/multimodal_data_discordance/data")
OUTPUT_ALL = OUTPUT_DIR / "unified_event_table.parquet"

df = pd.read_parquet(OUTPUT_ALL)

df.modality.value_counts()

df_echo = df.copy()[df.modality=='echo']



df.item_label.value_counts()

df.modality.value_counts()