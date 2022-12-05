# fill_na

This repository is a filling nan's for any of pepsico's manufacturing lines.

it takes as an input the categorical and flow meter columns and threshold under the pre_p section in the config files.

it completes the following cases:

1. categorical features -- it completes missing data under the following conditions:
  a. before and after nan's values are identical
  b. the legth of the missing feature is under n seconds
  
2. flow meters -- completes missing values by linear manipulations under the following condition:
  a. it is under 45 seconds

The output of the fill_na's is then placed under the appropriate location in the AWS bucket and saves and image of it.
Later on it's downloaded locally

requirements:
- missingno
- boto3
- ES
- ED 

