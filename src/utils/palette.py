import seaborn as sns

PALETTE = sns.color_palette("tab10").as_hex()
PALETTE_DATASETS = {'train': PALETTE[0], 'test': PALETTE[1], 'validation': PALETTE[2]}
PALETTE_TARGET = {'home': PALETTE[2], 'draw': PALETTE[1], 'away': PALETTE[4]}


