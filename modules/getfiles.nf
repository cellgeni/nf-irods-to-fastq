// functrion with error message if there are MD5 sums do not match
def downloadCramError(sample) {
    log.warn "md5sum conflict encountered for sample $sample"
    return 'ignore'
}


// Download a specified CRAM
// Perform the md5sum check locally rather than via iget -K
// There was a time where irods would bug out and not report an error when there was one
process downloadCram {
    label "normal4core"
    errorStrategy {task.exitStatus == 1 ? downloadCramError(sample) : 'terminate'}
    maxForks 10
    input:
        val meta
    output:
        tuple path("*.cram"), val(meta)
    script:
        """
        iget ${meta['cram_path']}
        FID=`basename ${meta['cram_path']}`
        MD5LOCAL=`md5sum \$FID | cut -f -1 -d " "`
        if [ \$MD5LOCAL != ${meta['md5']} ]
        then
            exit 1
        fi
        """
}


// Convert CRAM to FASTQ, using the numbering in the names for tidy S and L numbering
// Possibly publish, depending on what the input parameter says
// Accept I1/I2/R1/R2 output names in order as ATAC wants them named I1/R2/R1/R3 instead
// As a reminder, Nextflow variables are called as ${}
// Meanwhile bash variables are called as \$
// There's no need to escape underscores after Nextflow variables
// Meanwhile underscores after bash variables need to be escaped via \\_
// (A single \_ won't work here)
// The versions of stuff I have on the farm generate gibberish in I2 for single-index
// As such, need to check whether the CRAM is single index if the formula is unset
// Indices live in the BC tag, and a dual index is signalled by the presence of "-"
// Remove any empty (index) files at the end, let's assume no more than 50 bytes big
process cramToFastq {
    publishDir "${params.publish_dir}", mode: "copy", overwrite: true
    label "normal4core"
    container = '/nfs/cellgeni/singularity/images/samtools_v1.18-biobambam2_v2.0.183.sif'
    input:
        tuple path(cram_file), val(meta)
    output:
        tuple path("*.fastq.gz"), val(meta)
    script:
        """
        export REF_PATH=${params.REF_PATH}
        ISTRING="${params.index_format}"
        if [[ \$ISTRING == "i*i*" ]]
        then
            if [[ `samtools view $cram_file | grep "BC:" | head -n 1 | sed "s/.*BC:Z://" | sed "s/\\t.*//" | tr -dc "-" | wc -c` == 0 ]]
            then
                ISTRING="i*"
            fi
        fi
        if [[ `samtools view -H $cram_file | grep '@SQ' | wc -l` == 0 ]]
        then
            samtools fastq -@ ${task.cpus} -1 ${meta['fastq_name']}_R1_001.fastq.gz -2 ${meta['fastq_name']}_R2_001.fastq.gz --i1 ${meta['fastq_name']}_I1_001.fastq.gz --i2 ${meta['fastq_name']}_I2_001.fastq.gz --index-format \$ISTRING -n $cram_file
        else
            samtools view -b $cram_file | bamcollate2 collate=1 reset=1 resetaux=0 auxfilter=RG,BC,QT | samtools fastq -@ ${task.cpus} -1 ${meta['fastq_name']}_R1_001.fastq.gz -2 ${meta['fastq_name']}_R2_001.fastq.gz --i1 ${meta['fastq_name']}_I1_001.fastq.gz --i2 ${meta['fastq_name']}_I2_001.fastq.gz --index-format \$ISTRING -n -
        fi
        find . -type f -name "*.fastq.gz" -size -50c -exec rm {} \\;
        """
}