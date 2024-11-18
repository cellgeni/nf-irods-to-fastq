import groovy.json.JsonOutput

// Functrion with error message if there are MD5 sums do not match
def downloadCramError(sample) {
    log.warn "md5sum conflict encountered for sample $sample"
    return 'ignore'
}


// Function checks if a sample is generated by 10X ATAC kit
def checkATAC(library_type, i2len, fastq_prefix) {
    if (library_type.toLowerCase().contains('atac') && i2len == '24') {
        return true
    } else {
        return false
    }
}

// Download a specified CRAM
// Perform the md5sum check locally rather than via iget -K
// There was a time where irods would bug out and not report an error when there was one
process downloadCram {
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
    input:
        tuple path(cram_file), val(meta)
    output:
        tuple path("*.fastq.gz"), val(meta), env(num_reads_processed)
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
            samtools fastq -@ ${task.cpus} -1 ${meta['fastq_prefix']}_R1_001.fastq.gz -2 ${meta['fastq_prefix']}_R2_001.fastq.gz --i1 ${meta['fastq_prefix']}_I1_001.fastq.gz --i2 ${meta['fastq_prefix']}_I2_001.fastq.gz --index-format \$ISTRING -n $cram_file
        else
            samtools view -b $cram_file | bamcollate2 collate=1 reset=1 resetaux=0 auxfilter=RG,BC,QT | samtools fastq -@ ${task.cpus} -1 ${meta['fastq_prefix']}_R1_001.fastq.gz -2 ${meta['fastq_prefix']}_R2_001.fastq.gz --i1 ${meta['fastq_prefix']}_I1_001.fastq.gz --i2 ${meta['fastq_prefix']}_I2_001.fastq.gz --index-format \$ISTRING -n -
        fi
        find . -type f -name "*.fastq.gz" -size -50c -exec rm {} \\;
        num_reads_processed=\$(grep "processed" .command.log | sed 's/.*processed //; s/ reads//')
        """
}

// Calculate a read length for each .fastq.gz file
// Regular expression in sed command matches a read type for fastq name of format
// [Sample Name]_S[Sample Number]_L00[Lane number]_[Read type]_001.fastq.gz
process calculateReadLength {
    input:
        tuple path("*"), val(meta)
    output:
        tuple path("*.fastq.gz", includeInputs: true), val(meta), env(r1len), env(r2len), env(i1len), env(i2len)
    script:
        """
        export r1len="—" r2len="—" i1len="—" i2len="—"
        for file in *.fastq.gz
        do
            readtype=\$(echo \$file | sed -r 's/.*_L[0-9]{3}_([RI][12])_001.fastq.gz/\\1/')
            export "\${readtype,,}len"=\$(zcat ${meta["fastq_prefix"]}_\${readtype}_001.fastq.gz | awk 'NR%4==2' | head -1000 | awk '{sum += length(\$0) } END {print sum/NR}')
        done
        """
}

// Rename 10X ATAC files according to CellRanger naming convention
process renameATAC{
    label "local"
    tag "Renaming ATAC files"
    input:
        tuple path("*"), val(meta)
    output:
        tuple path('*.fastq.gz', includeInputs: true), val(meta)
    script:
        """
        rename 's/_R2_/_R3_/' ${meta["fastq_prefix"]}_R2_001.fastq.gz
        rename 's/_I2_/_R2_/' ${meta["fastq_prefix"]}_I2_001.fastq.gz
        """
}

// Save all metadata to Json File
process saveMetaToJson {
    input:
        tuple path(fastq, stageAs: "*.fastq.gz"), val(meta)
    output:
        path '*.json'
    exec:
        // Convert the map to JSON format
        def jsonContent = JsonOutput.toJson(meta)
        // Formats the JSON string to be more human-readable
        def prettyJson = JsonOutput.prettyPrint(jsonContent)
        // Write the JSON content to a file
        def jsonFile = new File("${task.workDir}/${meta['fastq_prefix']}.json")
        // The groovy script execution is happening in working directory of the script
        // So we need to move the .json file to the correct dirrectory
        jsonFile.write(prettyJson)
}