## u_create_override_meso.py
## Author: ccai (2025)

from modules.FN import FreightNetwork

import math
import time

if __name__ == "__main__":

    start_time = time.time()

    FN = FreightNetwork()
    FN.generate_mfhn()
    FN.create_override_meso()

    end_time = time.time()
    total_time = round(end_time - start_time)
    minutes = math.floor(total_time / 60)
    seconds = total_time % 60

    print(f"{minutes}m {seconds}s to execute.")

    print("Done")