// Get a Sample name from fastq path if fastq_path is in format
// path/to/dir/[Sample Name]_S[Sample Number]_L00[Lane number]_[Read type]_001.fastq.gz
// .*\/(.*?)_: Matches the part of the string after the last / but before _.
// _S.+_L\d{3}: Matches _S followed by the sample number and _L00 followed by three digits (representing the lane number)
// [RI]\d_001\.fastq\.gz: Matches the "Read Type" (R1, R2, I1, I2) and the rest of the file extension.
def getSampleName(fastq_path) {
    def match = fastq_path =~ /.*\/(.*?)_S.+_L\d{3}_[RI]\d_001\.fastq\.gz/
    return match[0][1]
}

// Concat fastq files before publishing them
process concatFastqs {
    input:
        tuple val(sample), path(fastqs)
    output:
        path("merged/*.fastq.gz")
    script:
        """
        mkdir -p merged
        # create list of files per read R1/R2 and index I1
        list_r1=\$(echo ${sample}*_S*_L*_R1_*.fastq.gz)
        list_r2=\$(echo ${sample}*_S*_L*_R2_*.fastq.gz)
        list_i1=\$(echo ${sample}*_S*_L*_I1_*.fastq.gz)

        # because not all samples will have R3 or I2 use shopt nullglob
        # to allow * expansion and not fail when it doesn't match anything
	    list_r3=\$(shopt -s nullglob; echo ${sample}*_S*_L*_R3_*.fastq.gz)
        list_i2=\$(shopt -s nullglob; echo ${sample}*_S*_L*_I2_*.fastq.gz)
        
		# R1 and R2 for all modalities
        echo "  ...Concatenating \$list_r1 >> ${sample}_S1_L001_R1_001.fastq.gz"
        cat \$list_r1 > merged/${sample}_S1_L001_R1_001.fastq.gz
        echo "  ...Concatenating \$list_r2 >> ${sample}_S1_L001_R2_001.fastq.gz"
        cat \$list_r2 > merged/${sample}_S1_L001_R2_001.fastq.gz
        
        # ATAC has R3 if present concatenate too
        if [[ ! -z "\$list_r3" ]]; then
            echo "  ...Concatenating \$list_r3 >> ${sample}_S1_L001_R3_001.fastq.gz"
            cat \$list_r3 > merged/${sample}_S1_L001_R3_001.fastq.gz
        fi

        # I1 for all modalities
        echo "  ...Concatenating \$list_i1 >> ${sample}_S1_L001_I1_001.fastq.gz"
        cat \$list_i1 > merged/${sample}_S1_L001_I1_001.fastq.gz
        
        ## we actually just ignore I2
        ## check if list_i2 has any matches
        ##if [[ ! -z "\$list_i2" ]]; then
        ##    echo "  ...Concatenating \$list_i2 >> ${sample}_S1_L001_I2_001.fastq.gz"
        ##    cat \$list_i2 > merged/${sample}_S1_L001_I2_001.fastq.gz
        ##fi
        """
}

// Upload fastq files to FTP
process uploadFTP {
    input:
        path(fastqs)
    output:
        path("done")
    script:
        """
        lftp -u ${params.ftp_credentials} ${params.ftp_host} -e "set ftp:ssl-allow no; cd ${params.ftp_path}; mput ${fastqs}; bye"
        >done
        """
}