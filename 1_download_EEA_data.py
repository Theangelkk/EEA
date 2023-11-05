# conda activate EEA

# Libreries
import sys
import os
import numpy as np
import pandas as pd
import airbase
import matplotlib.pyplot as plt

def joinpath(rootdir, targetdir):
    return os.path.join(os.sep, rootdir + os.sep, targetdir)

# Main data directory 
DATADIR = joinpath(os.getcwd() , "EEA_data")

if not os.path.exists(DATADIR):
  os.mkdir(DATADIR)