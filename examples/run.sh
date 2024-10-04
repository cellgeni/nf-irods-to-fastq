#!/bin/bash

set -eo pipefail

# LSF group is set and visible for nextflow job submissions
export LSB_DEFAULT_USERGROUP=<YOURGROUP>

# add Singularity to the the path
export PATH="/software/singularity/v3.10.0/bin:$PATH"

#input file, CSV with irods metadata
META="/path/to/samples.csv"

nextflow run main.nf \
    --findmeta "${META}" \
    --cram2fastq \
    -resume
