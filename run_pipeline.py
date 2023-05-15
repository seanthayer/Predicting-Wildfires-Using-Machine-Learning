# # #            # # #
#                    #
# Top-level pipeline #
#                    #
# # #            # # #

import sys
import os
os.chdir("./data")
sys.path.append(os.getcwd())
os.chdir("../")
sys.path.append(os.getcwd())

import dataset_retrieve_NOAA as process_NOAA        # type: ignore
import dataset_retrieve_NIFC as process_NIFC        # type: ignore
import dataset_join_incident_matrix as process_join # type: ignore
import dataset_discretize as process_discretize     # type: ignore
import dataset_join_unit_grid as process_unit_grid  # type: ignore

from datetime import datetime

# # #

def main():

  t_0 = datetime.now()

  print("[PIPELINE] Retrieving NOAA climate dataset")

  process_NOAA.main()

  print("[PIPELINE] Completed NOAA retrieval")

  print()

  print("[PIPELINE] Retrieving NIFC fire incidence dataset")

  process_NIFC.main()

  print("[PIPELINE] Completed NIFC retrieval")

  print()

  print("[PIPELINE] Joining NOAA <-> NIFC")

  process_join.main()

  print("[PIPELINE] Completed join")

  print()

  print("[PIPELINE] Generating unit grid")

  process_discretize.main()

  print("[PIPELINE] Completed generation")

  print()

  print("[PIPELINE] Joining Unit Grid <-> Incident Matrix")

  process_unit_grid.main()

  print("[PIPELINE] Completed join")

  print()

  dt = datetime.now() - t_0

  print("[PIPELINE] Completed pipeline in {}".format(dt))

  return 0

# # #

if __name__ == "__main__":
  sys.exit(main())