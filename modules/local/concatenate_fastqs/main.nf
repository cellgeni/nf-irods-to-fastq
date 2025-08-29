process CONCATENATE_FASTQS {
    tag "Concatenating FASTQ files for sample ${meta.id}"
    
    input:
    tuple val(meta), path(fastqs)

    output:
    tuple val(meta),  path("concatenated/*.fastq.gz"), emit: fastq

    script:
    """
    mkdir -p concatenated
    # create list of files per read R1/R2 and index I1
    list_r1=\$(echo *_R1*.fastq.gz)
    list_r2=\$(echo *_R2*.fastq.gz)
    list_i1=\$(echo *_I1*.fastq.gz)

    # because not all samples will have R3 or I2 use shopt nullglob
    # to allow * expansion and not fail when it doesn't match anything
    list_r3=\$(shopt -s nullglob; echo *_R3*.fastq.gz)
    list_i2=\$(shopt -s nullglob; echo *_I2*.fastq.gz)
    
    # R1 and R2 for all modalities
    echo "  ...Concatenating \$list_r1 >> ${meta.id}_S1_L001_R1_001.fastq.gz"
    cat \$list_r1 > concatenated/${meta.id}_S1_L001_R1_001.fastq.gz
    echo "  ...Concatenating \$list_r2 >> ${meta.id}_S1_L001_R2_001.fastq.gz"
    cat \$list_r2 > concatenated/${meta.id}_S1_L001_R2_001.fastq.gz
    
    # ATAC has R3 if present concatenate too
    if [[ ! -z "\$list_r3" ]]; then
        echo "  ...Concatenating \$list_r3 >> ${meta.id}_S1_L001_R3_001.fastq.gz"
        cat \$list_r3 > concatenated/${meta.id}_S1_L001_R3_001.fastq.gz
    fi

    # I1 for all modalities
    echo "  ...Concatenating \$list_i1 >> ${meta.id}_S1_L001_I1_001.fastq.gz"
    cat \$list_i1 > concatenated/${meta.id}_S1_L001_I1_001.fastq.gz
    
    ## we actually just ignore I2
    ## check if list_i2 has any matches
    ##if [[ ! -z "\$list_i2" ]]; then
    ##    echo "  ...Concatenating \$list_i2 >> ${meta.id}_S1_L001_I2_001.fastq.gz"
    ##    cat \$list_i2 > concatenated/${meta.id}_S1_L001_I2_001.fastq.gz
    ##fi
    """
    
    stub:
    """
    mkdir -p concatenated
    touch concatenated/${meta.id}_S1_L001_R1_001.fastq.gz
    touch concatenated/${meta.id}_S1_L001_R2_001.fastq.gz
    touch concatenated/${meta.id}_S1_L001_R3_001.fastq.gz
    touch concatenated/${meta.id}_S1_L001_I1_001.fastq.gz
    """
    
}