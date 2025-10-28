include { IRODS_FIND } from '../../../modules/local/irods/find'
include { IRODS_GETMETADATA } from '../../../modules/local/irods/getmetadata'
include { COMBINE_METADATA } from '../../../modules/local/combine_metadata/main'


def matchFilePatterns(path, patterns) {
    return patterns.any { pattern -> path ==~ pattern.replace('*', '.*') }
}

def makeFastqPrefix(metadata_list) {
    Map<String, List> cramDict = [:].withDefault { [] }

    def sortedMetadata = metadata_list.sort { m ->
        "${m.id_run}_${(m.lane ?: '1')}".toString()
    }

    sortedMetadata.each { meta ->
        String runidLane = "${meta.id_run}_${(meta.lane ?: '1')}".toString()
        cramDict[runidLane] << meta.tag_index

        def sampleName = meta.sample
        def sampleIdx  = cramDict[runidLane].size()
        def laneIdx    = cramDict.size() // number of unique runid_lane seen so far

        meta.fastq_prefix = String.format("%s_S%d_L%03d", sampleName, sampleIdx, laneIdx)
    }
    return metadata_list
}


workflow IRODS_FINDCRAMS {
    take:
    samples // channel of iRODS metadata maps to search for
    ignore_patterns // comma separated list of patterns to ignore when finding cram files, e.g. *.bai,*.crai,*.fastq.gz

    main:
    // convert ignore patterns to a list
    ignore_list = ignore_patterns ? ignore_patterns.split(',').collect { it.trim() }.findAll { it } : []

    // find all cram files for all samples
    IRODS_FIND(samples)
    crampaths = IRODS_FIND.out.csv
        // Get a list of cram files from results.csv file
        .splitCsv()
        // Filter files and create a grouping key
        .map { fullmeta, irodslist ->
            def filtered_list = irodslist.findAll { !matchFilePatterns(it, ignore_list) }
            def meta = [id: fullmeta.id]
            tuple( groupKey(meta, filtered_list.size()), filtered_list )
        }
        // Flatten the channel from [meta, [path1, path2, ...]] -> [meta, path1], [meta, path2], ...
        .transpose()

    // get metadata for samples
    IRODS_GETMETADATA(crampaths)

    // Create a prefix for a future fastq file
    metadata = IRODS_GETMETADATA.out.tsv
        // Read irods metadata from tsv file
        .splitCsv(header: true, sep: '\t')
        // Add cram path to metadata map
        .map { groupkey, irodspath, irodsmeta -> 
            def meta = [id: irodsmeta.sample] + irodsmeta + [cram_path: irodspath]
            tuple(groupkey, meta)
        }
        // Group channel by sample name
        .groupTuple(sort: 'hash')
        .map { _groupkey, metalist ->
            def updated_metalist = makeFastqPrefix(metalist)
            updated_metalist
        }
        .collect(sort: true)

    COMBINE_METADATA(metadata)

    // Collect versions files
    versions = IRODS_FIND.out.versions.first()
        .mix(
            IRODS_GETMETADATA.out.versions.first(),
            COMBINE_METADATA.out.versions
        )

    emit:
    metadata = COMBINE_METADATA.out.csv
    versions = versions
}