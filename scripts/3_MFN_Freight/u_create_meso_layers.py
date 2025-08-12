## u_create_meso_layers.py
## a translation of process_futureLinks.R
## Author: kcazzato
## Translated + Updated by ccai (2025) 

from modules.FN import FreightNetwork

import math
import time

if __name__ == "__main__":

    start_time = time.time()

    FN = FreightNetwork()
    FN.generate_mfhn()
    FN.check_mfn_fcs()
    FN.create_meso_layers()

    end_time = time.time()
    total_time = round(end_time - start_time)
    minutes = math.floor(total_time / 60)
    seconds = total_time % 60

    print(f"{minutes}m {seconds}s to execute.")

    print("Done")