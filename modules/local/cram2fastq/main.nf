process CRAM2FASTQ {
    tag "Converting CRAM to FASTQ for sample ${meta.id}"

    container 'docker://quay.io/cellgeni/reprocess_10x:latest'

    input:
    tuple val(meta), path(cram)

    output:
    tuple val(meta), path("*.fastq.gz"), env('num_reads_processed'), env('r1len'), env('r2len'), env('i1len'), env('i2len'), emit: fastq
    path "versions.yml", emit: versions

    script:
    def refpath = task.ext.refpath ?: ''
    def index_format = task.ext.index_format ?: 'i*i*'
    def format_atac = task.ext.format_atac ?: false
    """
    # Set reference path for samtools
    export REF_PATH="${refpath}"

    # Set index-format pattern (influences barcode handling)
    ISTRING="${index_format}"
    if [[ \$ISTRING == "i*i*" ]]
    then
        if [[ `samtools view $cram | grep "BC:" | head -n 1 | sed "s/.*BC:Z://" | sed "s/\\t.*//" | tr -dc "-" | wc -c` == 0 ]]
        then
            ISTRING="i*"
        fi
    fi

    # Run samtools
    if [[ `samtools view -H $cram | grep '@SQ' | wc -l` == 0 ]]
    then
        samtools fastq -@ ${task.cpus} -1 ${meta.id}_R1_001.fastq.gz -2 ${meta.id}_R2_001.fastq.gz --i1 ${meta.id}_I1_001.fastq.gz --i2 ${meta.id}_I2_001.fastq.gz --index-format \$ISTRING -n $cram
    else
        samtools view -b $cram | bamcollate2 collate=1 reset=1 resetaux=0 auxfilter=RG,BC,QT | samtools fastq -@ ${task.cpus} -1 ${meta.id}_R1_001.fastq.gz -2 ${meta.id}_R2_001.fastq.gz --i1 ${meta.id}_I1_001.fastq.gz --i2 ${meta.id}_I2_001.fastq.gz --index-format \$ISTRING -n -
    fi

    # Check if there are empty fastq files and delete them
    emptyfiles=\$(find . -type f -name "*.fastq.gz" -size -50c)
    if [[ -n "\$emptyfiles" ]]
    then
        echo "Warning: Found empty FASTQ files (< 50 bytes):"
        echo "\$emptyfiles"
        echo "Removing empty files..."
        find . -type f -name "*.fastq.gz" -size -50c -delete
        echo "Empty files removed."
    else
        echo "No empty FASTQ files found."
    fi  


    # Get number of processed reads
    num_reads_processed=\$(grep "processed" .command.log | sed 's/.*processed //; s/ reads//')
    
    # Calculate read length
    export r1len="—" r2len="—" i1len="—" i2len="—"
    for file in *.fastq.gz
    do
        readtype=\$(echo \$file | sed -r 's/.*_L[0-9]{3}_([RI][12])_001.fastq.gz/\\1/')
        export "\${readtype,,}len"=\$(zcat ${meta.id}_\${readtype}_001.fastq.gz | awk 'NR%4==2' | head -1000 | awk '{sum += length(\$0) } END {print sum/NR}')
    done
    echo "Read lengths (first 1000 reads):"
    echo "R1: \$r1len"
    echo "R2: \$r2len"
    echo "I1: \$i1len"
    echo "I2: \$i2len"

    # Rename atac if specified, library type mention atac and i2len equals 16 or 24
    library_type="${meta.library_type}"
    if [[ $format_atac == true && "\${library_type,,}" == *"atac"* && ( \$i2len -eq 16 || \$i2len -eq 24 ) ]]
    then
        rename 's/_R2_/_R3_/' ${meta.id}_R2_001.fastq.gz
        rename 's/_I2_/_R2_/' ${meta.id}_I2_001.fastq.gz
    fi

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        samtools: \$( samtools --version | grep samtools | awk '{print \$2}' )
        htslib: \$( samtools --version | grep "Using htslib" | awk '{print \$3}' )
        biobambam2: \$( bamcollate2 --version 2>&1 | head -n 1 | awk '{print \$5}' )
    END_VERSIONS
    """
}