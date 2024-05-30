#!/usr/bin/env nextflow

///////////////////////////////////////////////////////////////////////////////
// Get CRAMs from iRODS and convert them to fastq 
// Logic based on mapcloud CRAM downloader/converter
// https://github.com/Teichlab/mapcloud/tree/58b1d7163de7b0b2b8880fad19d722b586fc32b9/scripts/10x/utils
// Author: kp9
 ///////////////////////////////////////////////////////////////////////////////
 
nextflow.enable.dsl=2

def helpMessage() {
    log.info"""
    =======================
    iRODS to FASTQ pipeline
    =======================
    This pipeline pulls samples from iRODS and converts them to fastq files.
    Usage: nextflow run main.nf --meta /path/to/samples.csv

    == samples.csv format ==
    sample
    HCA_10687915
    HCA_10687916
    ========================
    """.stripIndent()
}

// Prepare a list of the CRAMs for the sample
// Store the sample ID and the CRAM path in a CSV file for subsequent merging
process findCrams {
    label "normal"
    maxForks 10
    input:
        tuple val(sample), val(val2), val(val3), val(samplemeta), val(meta2), val(meta3)
    output:
        path("${sample}_${val2}_${val3}.csv")
    script:
        """
        imeta qu -z seq -d ${samplemeta} = "${sample}" and ${meta2} = "${val2}" and ${meta3} = "${val3}" | \
            grep -v "No rows found" | \
            sed 's/collection: //g' | \
            sed 's/dataObj: //g' | \
            grep -v -- '---' | \
            paste -d '/' - - | \
            grep -v "#888.cram" | \
            grep -v "yhuman" | \
            sed "s/^/${sample},/" > "${sample}_${val2}_${val3}.csv"
        """
}

// Merge the per-sample CRAM lists into a single massive list across all samples
// We might be publishing this, and ending our run here
process combineCramLists {
    label "normal"
    publishDir "crams", mode: "copy", enabled: "${params.find_crams_only}"
    input:
        path(lists, stageAs: "input/*")
    output:
        path("crams.csv")
    script:
        """
        cat input/*.csv > crams.csv
        """
}

// Download a specified CRAM
// Perform the md5sum check locally rather than via iget -K
// There was a time where irods would bug out and not report an error when there was one
process downloadCram {
    label "normal4core"
    maxForks 10
    input:
        tuple val(sample), val(irods)
    output:
        tuple val(sample), path("*.cram")
    script:
        """
        iget ${irods}
        FID=`basename ${irods}`
        MD5LOCAL=`md5sum \$FID | cut -f -1 -d " "`
        MD5IRODS=`imeta ls -d ${irods} md5 | grep 'value:' | sed 's/value: //'`
        if [ \$MD5LOCAL != \$MD5IRODS ]
        then
            echo "md5sum conflict encountered for \$FID" 1>&2
            exit 1
        fi
        """
}

// Rename the CRAMs with tidy lane/sample counters for FASTQ generation
// Treat each RUN_LANE combination as a lane, and the number after that as a sample
process renameCram {
    label "normal"
    input:
        tuple val(sample), path(crams, stageAs: "input/*")
    output:
        tuple val(sample), path("*.cram")
    script:
        """
        lcount=1
        for LANE in `ls input/*.cram | sed "s|input/||" | cut -f 1 -d "#" | sort | uniq`
        do
            scount=1
            for FID in `ls input/\$LANE*.cram`
            do
                ln -s \$FID \$lcount#\$scount.cram
                (( scount++ ))
            done
            (( lcount++ ))
        done
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
    label "normal4core"
    container = '/nfs/cellgeni/singularity/images/samtools_v1.18-biobambam2_v2.0.183.sif'
    publishDir "${sample}", mode: "copy", enabled: "(${params.publish_fastqs} & (${params.aexpress_upload} == 'false'))"
    input:
        tuple val(sample), path(cram), val(i1), val(i2), val(r1), val(r2)
    output:
        tuple val(sample), path("*.fastq.gz")
    script:
        """
        export REF_PATH=${params.REF_PATH}
        scount=`basename ${cram} .cram | cut -f 2 -d "#"`
        lcount=`basename ${cram} .cram | cut -f 1 -d "#"`
        ISTRING="${params.index_format}"
        if [[ \$ISTRING == "i*i*" ]]
        then
            if [[ `samtools view ${cram} | grep "BC:" | head -n 1 | sed "s/.*BC:Z://" | sed "s/\\t.*//" | tr -dc "-" | wc -c` == 0 ]]
            then
                ISTRING="i*"
            fi
        fi
        if [[ `samtools view -H ${cram} | grep '@SQ' | wc -l` == 0 ]]
        then
            samtools fastq -@ ${task.cpus} -1 ${sample}_S\$scount\\_L00\$lcount\\_${r1}_001.fastq.gz -2 ${sample}_S\$scount\\_L00\$lcount\\_${r2}_001.fastq.gz --i1 ${sample}_S\$scount\\_L00\$lcount\\_${i1}_001.fastq.gz --i2 ${sample}_S\$scount\\_L00\$lcount\\_${i2}_001.fastq.gz --index-format \$ISTRING -n ${cram}
        else
            samtools view -b ${cram} | bamcollate2 collate=1 reset=1 resetaux=0 auxfilter=RG,BC,QT | samtools fastq -1 ${sample}_S\$scount\\_L00\$lcount\\_${r1}_001.fastq.gz -2 ${sample}_S\$scount\\_L00\$lcount\\_${r2}_001.fastq.gz --i1 ${sample}_S\$scount\\_L00\$lcount\\_${i1}_001.fastq.gz --i2 ${sample}_S\$scount\\_L00\$lcount\\_${i2}_001.fastq.gz --index-format \$ISTRING -n -
        fi
        find . -type f -name "*.fastq.gz" -size -50c -exec rm {} \\;
        """
}

process concatFastqs {
    label "normal"
    publishDir "${launchDir}", pattern: 'arrayexpress/*.fastq.gz', mode: 'copy', enabled: "${params.aexpress_upload} == 'manual'"
    input:
        tuple val(sample), path(fastqs)
    output:
        path("arrayexpress/*.fastq.gz")
    script:
        """
        mkdir -p arrayexpress
        # create list of files per read
        list_r1=\$(echo ${sample}*_S*_L*_R1_*.fastq.gz)
        list_r2=\$(echo ${sample}*_S*_L*_R2_*.fastq.gz)
        list_i1=\$(echo ${sample}*_S*_L*_I1_*.fastq.gz)
        # shopt nullglob allows * expansion not to fail when it doesn't match anything
        list_i2=\$(shopt -s nullglob; echo ${sample}*_S*_L*_I2_*.fastq.gz)
        cat \$list_r1 > arrayexpress/${sample}_S1_L001_R1_001.fastq.gz
        cat \$list_r2 > arrayexpress/${sample}_S1_L001_R2_001.fastq.gz
        cat \$list_i1 > arrayexpress/${sample}_S1_L001_I1_001.fastq.gz
        # check if list_i2 has any matches
        if [[ ! -z "\$list_i2" ]]; then
            echo "  ...Concatenating I2"
            cat \$list_i2 > arrayexpress/${sample}_S1_L001_I2_001.fastq.gz
        fi
        """
}

process uploadtoArrayExpress {
    label "normal"
    input:
        path(fastqs)
    output:
        path("done.txt")
    script:
        """
        lftp -u ${params.aexpress_credentials} ${params.aexpress_address} -e "set ftp:ssl-allow no; cd ${params.aexpress_directory}; mput ${fastqs}; bye"
        touch done.txt
        """
}

// Helper workflow that creates a master CRAM list based on the metadata file
workflow findcrams {
    main:
        // We need some sort of sample information to download
        if (params.meta == null) {
            error "Please provide a metadata file via --meta"
        }
        // Get the header line of the CSV
        // If fewer than three entries are present, buffer with "default" iRODS filler
        // i.e. target = 1 and type = cram
        // Accomplish this by appending the extras at the end and then subsetting to 3
        fields = Channel
            .fromPath(params.meta, checkIfExists: true)
            .splitCsv()
            .first()
            .combine(Channel.of("target"))
            .combine(Channel.of("type"))
            .map({it -> [it[0], it[1], it[2]]})
        // Load contents of CSV
        // Repeat same general idea of buffering with "default" iRODS filler
        // And then slice down to three entries per line
        // And then create six entries per line by adding in the header information
        metadata = Channel
            .fromPath(params.meta, checkIfExists: true)
            .splitCsv(skip:1)
            .combine(Channel.of("1"))
            .combine(Channel.of("cram"))
            .map({it -> [it[0], it[1], it[2]]})
            .combine(fields)
        // Perform the designated iRODS queries, getting a CSV out of each
        // With the CSV lines being sample,irods_path_to_cram
        // Put all the CSVs together...
        foundCrams = findCrams(metadata).collect()
        // ...and turn them into a mega-CSV with all the files to download
        cramList = combineCramLists(foundCrams)
    emit:
        cramList = cramList
}

// Specify a workflow name and what it emits and everything
// As this way its .out.fastqs can be used by other Nextflow stuff that imports it
workflow irods {
    main:
        // Do we need to find the CRAMs? If no CRAM list is passed then yes
        if (params.cram_list == null) {
            findcrams()
            cramList = findcrams.out.cramList
        } else {
            // We got given a CRAM list, load it
            cramList = Channel.fromPath(params.cram_list, checkIfExists: true)
        }        
        // Handily Nextflow channels that point to a file can have their contents read
        cramPaths = cramList.splitCsv()
        // Can now download the CRAMs
        // They're named somewhat chaotically, so rename them in an orderly fashion
        // So that the sample and lane counts (in case Cellranger becomes involved)
        // Increment nicely and tidily; more details as a comment before renameCram()
        // .groupTuple() to see all the CRAMs for a given sample at once
        crams = downloadCram(cramPaths).groupTuple()
        // .transpose() to effectively undo the groupTuple(), get one line per CRAM
        // Though with sample info still present in each line
        renamedCrams = renameCram(crams).transpose()
        // ATAC is kinda special and wants its reads named differently
        // The process expects samtools fastq output names in I1/I2/R1/R2 order
        if (params.type == "ATAC") {
            renamedCrams = renamedCrams
                .combine(Channel.of("I1"))
                .combine(Channel.of("R2"))
                .combine(Channel.of("R1"))
                .combine(Channel.of("R3"))
        }
        else {
            renamedCrams = renamedCrams
                .combine(Channel.of("I1"))
                .combine(Channel.of("I2"))
                .combine(Channel.of("R1"))
                .combine(Channel.of("R2"))
        }
        // Perform the conversion
        // Afterward group up the FASTQ lists by sample
        // And turn the FASTQ list of lists into a single list
        fastqs = cramToFastq(renamedCrams)
            .groupTuple()
            .map({it -> [it[0], it[1].flatten()]})
    emit:
        fastqs = fastqs
}

workflow arrayexpress {
    // Concatenate the multiple fastqs from each of I1/I2/R1/R2 to a single I1/I2/R1/R2
    concattedFastqs = concatFastqs(irods.out)
    //  Files are ready to upload
    if (params.upload_type == 'auto'){
        uploadtoArrayExpress(concattedFastqs)
    }

}

// Do this so the script actually runs
// The equivalent of python's main() thing, I guess
workflow {
    if (params.help) {
        helpMessage()
        exit 0
    }
    // Do we just return the CRAM list?
    if (params.find_crams_only == false) {
        // This needs to be a proper start to end workflow so fastqs can be emitted
        irods()
    } else {
        // This will find the CRAMs and publish the list
        findcrams()
    }

    // Do we need the FASTQs for ArrayExpress submission?
    if (params.aexpress_upload != 'false'){
        arrayexpress()
    }
}
