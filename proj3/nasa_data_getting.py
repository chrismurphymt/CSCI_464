import sys

import numpy as np

import netCDF4 as nc4

import matplotlib.pyplot as plt





rootgrp = nc4.Dataset('WC_MULTISEN_PREC_025_L3_V001_19980101T00Z.nc4', 'r')

print ("rootgrp.variables['T'].shape", rootgrp.variables['prec'].shape)


# read global air temperature for all levels

print ('Reading T...',); sys.stdout.flush()

T = rootgrp.variables['prec'][0,:,:]

print ('done.')
sys.stdout.flush()

print ('T.shape:', T.shape)
