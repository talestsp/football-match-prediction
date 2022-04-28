import seaborn as sns

PALETTE = sns.color_palette("tab10").as_hex()
DATASETS_PAL = {'train': PALETTE[0], 'test': PALETTE[1], 'validation': PALETTE[2]}