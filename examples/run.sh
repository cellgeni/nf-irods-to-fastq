#!/bin/bash

set -eo pipefail

module load cellgen/nextflow/24.10.0
module load cellgen/irods
module load cellgen/singularity
module load python-3.11.6

# LSF group is set and visible for nextflow job submissions
export LSB_DEFAULT_USERGROUP=<YOURGROUP>

#input file, CSV with irods metadata
META="/path/to/samples.csv"

nextflow run main.nf \
    --findmeta "${META}" \
    --cram2fastq \
    -resume
