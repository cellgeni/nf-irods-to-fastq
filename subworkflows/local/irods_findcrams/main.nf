include { IRODS_FIND } from '../../../modules/local/irods/find'
include { IRODS_GETMETADATA } from '../../../modules/local/irods/getmetadata'
include { COMBINE_METADATA } from '../../../modules/local/combine_metadata/main'


def mathFilePatterns(path, patterns) {
    return patterns.any { pattern -> path ==~ pattern.replace('*', '.*') }
}

def makeFastqPrefix(metadata_list) {
    // A map to track unique tags for each unique runid_lane
    def cramDict = [:]
    
    // Sort metadata list by run_id + lane
    def sortedMetadata = metadata_list.sort { meta -> 
        "${meta.id_run}_${meta.lane ?: '1'}"
    }
    
    // Make unique prefix for each metadata entry
    sortedMetadata.eachWithIndex { meta, globalIndex ->
        // Parse cram name to get run_id, lane and tag_id
        def runidLane = "${meta.id_run}_${meta.lane ?: '1'}"
        def tagId = meta.tag_index
        
        // Initialize list if not exists
        if (!cramDict.containsKey(runidLane)) {
            cramDict[runidLane] = []
        }
        
        // Add runid_lane + tag_id combination to the dict
        cramDict[runidLane].add(tagId)
        
        // Make a name for fastq file
        def sampleName = meta.sample
        def sampleIdx = cramDict[runidLane].size()
        def laneIdx = (globalIndex + 1).toString()
        
        meta.fastq_prefix = "${sampleName}_S${sampleIdx}_L${laneIdx.padLeft(3, '0')}"
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
            // Remove unnecessary metadata fields, filter files and create a grouping key
            .map { fullmeta, irodslist ->
                def filtered_list = irodslist.findAll { !mathFilePatterns(it, ignore_list) }
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
                def meta = irodsmeta + [cram_path: irodspath] + groupkey.getGroupTarget()
                tuple(groupkey, meta)
            }
            // Group channel by sample name
            .groupTuple()
            // Add a fastq prefix to metadata map by using sample name, lane, run_id and tag_id
            .map { _groupkey, metalist ->
                def updated_metalist = makeFastqPrefix(metalist)
                updated_metalist
            }
            .collect()

        COMBINE_METADATA(metadata)

        // Collect versions files
        versions = IRODS_FIND.out.versions.mix(IRODS_GETMETADATA.out.versions).mix(COMBINE_METADATA.out.versions)
    emit:
        metadata = COMBINE_METADATA.out.csv
        versions = versions
        
}